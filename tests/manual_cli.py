#!/usr/bin/env python3
import socket, struct

HOST, PORT = "127.0.0.1", 9999
OPCODES = {"SET": 0x02, "GET": 0x01, "DEL": 0x03}
STATUS = {0x00: "OK", 0x01: "NOT_FOUND", 0x02: "ERROR"}

def encode(cmd, key, val=b""):
    key = key.encode()
    return struct.pack("!BI", OPCODES[cmd], len(key)) + key + struct.pack("!I", len(val)) + val

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed connection")
        buf += chunk
    return buf

def send_cmd(sock, cmd, key, val=b""):
    sock.sendall(encode(cmd, key, val))
    status, val_len = struct.unpack("!BI", recv_exact(sock, 5))
    resp_val = recv_exact(sock, val_len) if val_len else b""
    return STATUS.get(status, f"UNKNOWN({status})"), resp_val

def main():
    sock = socket.create_connection((HOST, PORT))
    print(f"connected to {HOST}:{PORT} — SET key val | GET key | DEL key | quit")
    while True:
        try:
            line = input("pulsekv> ").strip()
        except EOFError:
            break
        if not line or line.lower() == "quit":
            break
        parts = line.split(maxsplit=2)
        cmd = parts[0].upper()
        if cmd not in OPCODES:
            print("unknown command"); continue
        key = parts[1] if len(parts) > 1 else ""
        val = parts[2].encode() if len(parts) > 2 else b""
        try:
            status, resp_val = send_cmd(sock, cmd, key, val)
        except ConnectionError as e:
            print(f"connection error: {e}"); break
        print(status + (f" -> {resp_val.decode(errors='replace')}" if resp_val else ""))
    sock.close()

if __name__ == "__main__":
    main()
