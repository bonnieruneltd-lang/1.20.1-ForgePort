#!/usr/bin/env python3
"""Tiny RCON client used by the end-to-end smoke test (no third-party dependencies)."""
import socket
import struct
import sys

HOST, PORT, PASSWORD = "127.0.0.1", 25575, "test"


def pack(request_id, kind, body):
    payload = struct.pack("<ii", request_id, kind) + body.encode("utf-8") + b"\x00\x00"
    return struct.pack("<i", len(payload)) + payload


def recv_packet(sock):
    raw = b""
    while len(raw) < 4:
        chunk = sock.recv(4 - len(raw))
        if not chunk:
            return None, None
        raw += chunk
    size = struct.unpack("<i", raw)[0]
    body = b""
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        if not chunk:
            return None, None
        body += chunk
    request_id, kind = struct.unpack("<ii", body[:8])
    return request_id, body[8:-2].decode("utf-8", "replace")


def main():
    commands = sys.argv[1:]
    sock = socket.create_connection((HOST, PORT), timeout=10)
    sock.sendall(pack(1, 3, PASSWORD))
    recv_packet(sock)
    for index, command in enumerate(commands, start=2):
        sock.sendall(pack(index, 2, command))
        _, response = recv_packet(sock)
        print(f"$ {command}\n{response.strip()}\n")
    sock.close()


if __name__ == "__main__":
    main()
