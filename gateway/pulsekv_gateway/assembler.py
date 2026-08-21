"""Order-preserving block and OpenAI-request assembly (Phase 10.5)."""

from __future__ import annotations

import copy
import json
from typing import Any, Mapping, MutableMapping, Sequence, Tuple

from .models import ContextBlock, GatewayError

__all__ = ["AssemblyError", "assemble", "assemble_request"]


class AssemblyError(GatewayError):
    """The decomposed request and substitution map do not agree safely."""


def assemble(
    blocks: Sequence[ContextBlock], substitutions: Mapping[int, str]
) -> Tuple[str, ...]:
    """Return block text in input order, replacing only accepted indices."""

    known: set[int] = set()
    output = []
    for block in blocks:
        if block.index in known:
            raise AssemblyError(f"duplicate block index {block.index}")
        known.add(block.index)
        replacement = substitutions.get(block.index, block.text)
        if not isinstance(replacement, str) or not replacement:
            raise AssemblyError(
                f"substitution for block {block.index} must be a non-empty string"
            )
        output.append(replacement)
    unknown = set(substitutions) - known
    if unknown:
        raise AssemblyError(f"substitutions reference unknown blocks {sorted(unknown)}")
    return tuple(output)


def assemble_request(
    request: Mapping[str, Any],
    blocks: Sequence[ContextBlock],
    substitutions: Mapping[int, str],
) -> Mapping[str, Any]:
    """Deep-copy an OpenAI request and substitute accepted blocks in place.

    Message order, tool order, and every unrelated field are retained. A tool
    replacement must decode to an object; invalid canonical JSON is an
    assembly error and therefore takes the server's fail-open path.
    """

    rendered = assemble(blocks, substitutions)
    if not substitutions:
        return request

    result = copy.deepcopy(dict(request))
    messages = result.get("messages", [])
    tools = result.get("tools", [])
    cursor = 0

    for message in messages:
        if not isinstance(message, MutableMapping):
            continue
        content = message.get("content")
        if not isinstance(content, str) or not content:
            continue
        _require_same_block(blocks, cursor, content)
        if cursor in substitutions:
            message["content"] = rendered[cursor]
        cursor += 1

    for position, tool in enumerate(tools):
        serialized = json.dumps(tool, ensure_ascii=False, sort_keys=False)
        _require_same_block(blocks, cursor, serialized)
        if cursor in substitutions:
            try:
                replacement = json.loads(rendered[cursor])
            except json.JSONDecodeError as exc:
                raise AssemblyError(
                    f"tool substitution for block {cursor} is not valid JSON"
                ) from exc
            if not isinstance(replacement, dict):
                raise AssemblyError(
                    f"tool substitution for block {cursor} must decode to an object"
                )
            tools[position] = replacement
        cursor += 1

    if cursor != len(blocks):
        raise AssemblyError(
            f"request contains {cursor} blocks but decomposer supplied {len(blocks)}"
        )
    return result


def _require_same_block(
    blocks: Sequence[ContextBlock], index: int, original_text: str
) -> None:
    if index >= len(blocks):
        raise AssemblyError(f"request contains unexpected block {index}")
    block = blocks[index]
    if block.index != index:
        raise AssemblyError(
            f"block sequence position {index} carries index {block.index}"
        )
    if block.text != original_text:
        raise AssemblyError(f"block {index} no longer matches the request")
