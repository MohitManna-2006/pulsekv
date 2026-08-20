"""Embedding encoder (Phase 10.3).

Design doc §16 (embedding model identity), §11 Tier 2; plan §7.

The model is chosen here, against measured data rather than assumption, and the
choice is recorded in ``docs/pulsekv-semantic-context-phase10.3-summary.md`` §2.

Whatever is chosen, ``model_id`` and ``model_version`` must be reported
truthfully and stamped onto every record embedded with it: design doc §16 makes
a vector produced by model A an invalid comparison against model B's, and the
registry record carries both fields so that mismatch is detectable rather than
silent.

The model: sentence-transformers/all-MiniLM-L6-v2, ONNX, CPU
------------------------------------------------------------
* **384 dimensions**, which is the width design doc §10 already assumed when it
  argued brute-force cosine needs no index at MVP scale ("a few thousand
  384-dimensional vectors").
* **Six layers, 22M parameters** -- the fastest widely-validated sentence
  encoder. Speed is the point: §19 says the single most important unmeasured
  number in this project is whether the gateway costs more than the prefill it
  saves, and this phase exists partly to start answering that honestly. Picking
  a slower, stronger encoder would have made the number worse; picking a
  static-embedding model would have made it flatteringly better while giving
  the guard weaker candidates. This is the middle.
* **Apache-2.0**, and its ONNX export is published in the model's own
  repository, so the artifact is reproducible by URL with no conversion step.

**The fp32 export is used deliberately, not the int8 one.** The repository also
ships ``model_qint8_arm64.onnx`` and ``model_quint8_avx2.onnx``, which are
faster. They are not used because quantized kernels differ per architecture: a
registry populated on an arm64 host and read by an x86 worker would compare
vectors that were never comparable, silently, which is exactly the drift design
doc §16 and risk register row 6 exist to prevent. Determinism is worth more
here than the speed, because the registry caches embeddings across restarts and
across machines.

**Upgrade trigger, named rather than left open:** if Phase 10.4's corpus shows
Tier 2 *recall* is the binding constraint -- true paraphrases not reaching the
guard at all -- ``BAAI/bge-small-en-v1.5`` is the same 384 dimensions with
better retrieval quality at roughly twice the layers. Do not swap on
intuition; a swap invalidates every stored embedding by design (§16).

Pooling is mean-over-attention-mask followed by L2 normalization, which is what
this model card specifies. The ONNX graph emits ``last_hidden_state``, so the
pooling is this module's to perform and therefore this module's to version --
see ``ENCODER_REVISION``.

Truncation, and why it matters downstream
-----------------------------------------
The model's positional limit is 512 tokens; the tokenizer shipped in the
repository is configured to truncate at **128**, which is the sentence-
transformers default and far too short for the block types this gateway
targets. It is reconfigured to 512 here.

Even at 512 the boundary is real and is **the most important thing Phase 10.4
inherits from this phase**: a block longer than 512 tokens is embedded from its
first 512 tokens only, so two long blocks that agree on their opening and
diverge afterwards will look nearly identical to Tier 2. Design doc §19's own
bypass threshold is stated in tokens *above* this limit, so the long blocks this
feature exists for are precisely the ones affected. ``count_tokens`` and
``max_sequence_tokens`` are exposed so the guard can see the boundary rather
than infer it.
"""

from __future__ import annotations

import hashlib
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Optional, Sequence, Tuple

from .models import GatewayError

__all__ = [
    "DEFAULT_MODEL_ID",
    "ENCODER_REVISION",
    "Encoder",
    "EncoderError",
    "EncoderUnavailableError",
    "OnnxEncoder",
    "cosine_similarity",
    "vector_from_bytes",
    "vector_to_bytes",
]

DEFAULT_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"

# Bump when anything about how a vector is produced changes -- pooling,
# normalization, or the truncation limit -- even if the weights do not. The
# weights are covered separately by their digest; this covers this module.
ENCODER_REVISION = "1"

# Vectors are stored in the registry's opaque `embedding` blob (Phase 10.1
# deliberately left the format to this phase). Little-endian float32, no
# header: the record's embedding_model_id/_version already pin the model, and
# therefore the width, so a header would restate what the contract carries. The
# decoder validates the length against the expected width regardless.
_VECTOR_DTYPE = "<f4"
_FLOAT_BYTES = 4


class EncoderError(GatewayError):
    """Base for encoder failures."""


class EncoderUnavailableError(EncoderError):
    """The encoder is unavailable or exceeded its latency budget.

    Design doc §17: tiers 2/3 are skipped for that request, anything already
    resolved by tiers 0/1 still applies, and everything else passes through
    unchanged.
    """


class Encoder:
    """Turns block text into a vector for Tier 2 retrieval.

    Base class. It owns two things every encoder must do identically -- the
    latency budget (design doc §17, risk register row 14's "real timeouts, not
    aspirational") and the refusal to return a malformed vector -- and delegates
    the model to ``_encode``.
    """

    def __init__(self, *, timeout_ms: Optional[int] = None) -> None:
        self._timeout_ms = timeout_ms
        self._pool: Optional[ThreadPoolExecutor] = None
        self._pool_lock = threading.Lock()

    @property
    def model_id(self) -> str:
        """Stamped onto every record this encoder embeds (design doc §16)."""
        raise NotImplementedError("use a concrete Encoder subclass")

    @property
    def model_version(self) -> str:
        """Stamped alongside ``model_id``; the two are meaningless apart."""
        raise NotImplementedError("use a concrete Encoder subclass")

    @property
    def dimension(self) -> int:
        """Vector width. Fixed by the model, checked on every encode."""
        raise NotImplementedError("use a concrete Encoder subclass")

    def encode(self, text: str) -> Sequence[float]:
        """Encode one block. Raises ``EncoderUnavailableError`` past budget.

        **Feed this ``normalizer.normalize_for_hash``'s output, not raw block
        text.** Tier 0 hashes the normalized form, so embedding the raw form
        would mean the two tiers disagree about what the block *is* -- the same
        block would have one identity for the exact tier and another for the
        semantic one.

        The budget is enforced by running the model on a worker thread and
        abandoning it, which bounds what the *caller* waits for. It does not
        abort the inference itself: ONNX Runtime is a synchronous C++ call with
        no cancellation, so an abandoned encode runs to completion in the
        background. It releases the GIL while it does, so it does not block
        other requests -- but a sustained overload is not cured by this
        timeout, only made survivable. Said plainly because risk register row
        14 asks for real timeouts, and this is exactly how real this one is.
        """
        try:
            if self._timeout_ms is None:
                vector = self._encode(text)
            else:
                future = self._executor().submit(self._encode, text)
                try:
                    vector = future.result(timeout=self._timeout_ms / 1000.0)
                except FutureTimeoutError as exc:
                    future.cancel()
                    raise EncoderUnavailableError(
                        f"{self.model_id}: encode exceeded its {self._timeout_ms} ms "
                        f"budget (design doc §17: Tier 2/3 are skipped for this block)"
                    ) from exc
        except EncoderError:
            raise
        except BaseException as exc:  # noqa: BLE001 -- normalized to a typed error
            # Every failure leaves here as an EncoderError, whether a budget was
            # configured or not. Without this the guarantee held only on the
            # timeout path, and a model that raised on an un-budgeted encoder
            # escaped straight through Matcher.resolve -- which is required
            # never to raise for an expected failure (design doc §17).
            raise EncoderUnavailableError(f"{self.model_id}: {exc}") from exc

        if len(vector) != self.dimension:
            raise EncoderUnavailableError(
                f"{self.model_id}: produced {len(vector)} dimensions, expected "
                f"{self.dimension}"
            )
        return vector

    def count_tokens(self, text: str) -> int:
        """Tokens this encoder would consume, before truncation.

        Exposed so a caller can see the truncation boundary rather than infer
        it; compare against ``max_sequence_tokens``.
        """
        raise NotImplementedError("use a concrete Encoder subclass")

    @property
    def max_sequence_tokens(self) -> int:
        """Tokens beyond which text is not seen by the model at all."""
        raise NotImplementedError("use a concrete Encoder subclass")

    def close(self) -> None:
        """Release the budget worker pool, if one was started."""
        with self._pool_lock:
            pool, self._pool = self._pool, None
        if pool is not None:
            pool.shutdown(wait=False)

    def __enter__(self) -> "Encoder":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    def _encode(self, text: str) -> Sequence[float]:
        raise NotImplementedError("use a concrete Encoder subclass")

    def _executor(self) -> ThreadPoolExecutor:
        with self._pool_lock:
            if self._pool is None:
                self._pool = ThreadPoolExecutor(
                    max_workers=4, thread_name_prefix="pulsekv-encoder"
                )
            return self._pool


class OnnxEncoder(Encoder):
    """all-MiniLM-L6-v2 over ONNX Runtime, CPU only.

    ``model_version`` is derived from the artifact rather than asserted: it is
    ``ENCODER_REVISION`` plus a digest of the weights file. Swapping
    ``model.onnx`` in place -- the drift that a hand-written version string
    would miss entirely -- changes the version, and every registry entry
    embedded with the old one stops being a comparable candidate, which is
    exactly what design doc §16 asks for.
    """

    def __init__(
        self,
        model_dir: "str | Path",
        *,
        model_id: str = DEFAULT_MODEL_ID,
        max_sequence_tokens: int = 512,
        timeout_ms: Optional[int] = None,
    ) -> None:
        super().__init__(timeout_ms=timeout_ms)
        self._model_dir = Path(model_dir).expanduser()
        self._model_id = model_id
        self._max_tokens = int(max_sequence_tokens)

        weights = self._model_dir / "model.onnx"
        tokenizer_file = self._model_dir / "tokenizer.json"
        for path in (weights, tokenizer_file):
            if not path.is_file():
                raise EncoderUnavailableError(
                    f"{path}: missing. Fetch the model with the command in "
                    f"gateway/README.md, or point PULSEKV_GATEWAY_MODEL_DIR at a "
                    f"directory that has it"
                )
        try:
            import numpy  # noqa: F401 -- imported here so the error is typed
            import onnxruntime
            from tokenizers import Tokenizer
        except ImportError as exc:
            raise EncoderUnavailableError(
                f"the embedding stack is not installed: {exc}"
            ) from exc

        self._numpy = numpy
        try:
            self._tokenizer = Tokenizer.from_file(str(tokenizer_file))
            # The repository ships this configured to 128, the sentence-
            # transformers default; the model itself has 512 positions. Padding
            # is switched off because every call here is a batch of one, and a
            # padded batch would only add masked positions to pool away.
            self._tokenizer.no_padding()
            self._tokenizer.enable_truncation(max_length=self._max_tokens)
            self._session = onnxruntime.InferenceSession(
                str(weights), providers=["CPUExecutionProvider"]
            )
        except Exception as exc:  # noqa: BLE001 -- any load failure is unavailability
            raise EncoderUnavailableError(f"{self._model_dir}: {exc}") from exc

        outputs = [o.shape for o in self._session.get_outputs()]
        self._dimension = int(outputs[0][-1])
        self._model_version = (
            f"{ENCODER_REVISION}+{_digest_file(weights)}"
        )

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def max_sequence_tokens(self) -> int:
        return self._max_tokens

    def count_tokens(self, text: str) -> int:
        # Truncation is on, so this reports what the model will actually see.
        # A caller comparing it to max_sequence_tokens learns whether the tail
        # of a long block was dropped.
        return len(self._tokenizer.encode(text).ids)

    def _encode(self, text: str) -> Sequence[float]:
        numpy = self._numpy
        encoded = self._tokenizer.encode(text)
        ids = numpy.array([encoded.ids], dtype=numpy.int64)
        mask = numpy.array([encoded.attention_mask], dtype=numpy.int64)
        try:
            hidden = self._session.run(
                ["last_hidden_state"],
                {
                    "input_ids": ids,
                    "attention_mask": mask,
                    # BERT-family graphs require this input; a single sequence
                    # is all segment zero.
                    "token_type_ids": numpy.zeros_like(ids),
                },
            )[0]
        except Exception as exc:  # noqa: BLE001
            raise EncoderUnavailableError(f"{self._model_id}: {exc}") from exc

        weights = mask[..., None].astype(numpy.float32)
        pooled = (hidden * weights).sum(axis=1) / numpy.clip(
            weights.sum(axis=1), 1e-9, None
        )
        vector = pooled[0]
        norm = float(numpy.linalg.norm(vector))
        if norm == 0.0:
            # Every token masked out: nothing to compare, and a zero vector
            # would have undefined cosine against everything.
            raise EncoderUnavailableError(
                f"{self._model_id}: text produced a zero vector"
            )
        return tuple(float(value) for value in vector / norm)


def vector_to_bytes(vector: Sequence[float]) -> bytes:
    """Serialize a vector for the registry's opaque ``embedding`` blob."""
    return struct.pack(f"<{len(vector)}f", *vector)


def vector_from_bytes(blob: bytes, dimension: int) -> Tuple[float, ...]:
    """Read a blob back, refusing one that is not the expected width.

    A wrong-width blob is a mismatch the model-identity stamps did not catch --
    a hand-written record, or a format change -- and reading it as a shorter
    vector would produce a similarity score against the wrong thing.
    """
    expected = dimension * _FLOAT_BYTES
    if len(blob) != expected:
        raise EncoderError(
            f"embedding blob is {len(blob)} bytes, expected {expected} for "
            f"{dimension} float32 dimensions"
        )
    return struct.unpack(f"<{dimension}f", blob)


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Cosine similarity, clamped into ``Candidate.similarity``'s [0, 1].

    Both operands are L2-normalized by the encoder, so this is their dot
    product. It is clamped rather than asserted because floating-point error
    can put a self-comparison a few ulps above 1.0, and ``Candidate``'s
    validator would refuse the record for a rounding artifact.

    Negative similarity clamps to 0.0. That loses the distinction between
    "unrelated" and "opposed", which costs nothing here: Tier 2 ranks
    candidates, and anything at or below zero is far past being one.
    """
    total = 0.0
    for a, b in zip(left, right):
        total += a * b
    return min(1.0, max(0.0, total))


def _digest_file(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()[:16]
