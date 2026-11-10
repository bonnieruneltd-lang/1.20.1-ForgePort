#!/usr/bin/env python3
"""Minimal Forge Talker / MultiPaper master stub used by the end-to-end smoke test.

It implements just enough of the protocol in docs/PROTOCOL.md to verify the node side:
  * answers HELLO with HELLO_ACK + an OWNERSHIP_SNAPSHOT
  * answers PING with PONG
  * logs every frame it receives (RoutedFrame/BlockMutation proves that the guard relayed a
    foreign mutation instead of applying it locally)

Usage: python3 scripts/fake_master.py <port> <logfile>
"""
import json
import socket
import struct
import sys
import threading
import time

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 23600
LOG = sys.argv[2] if len(sys.argv) > 2 else "/workspace/.logs/master.log"

EPOCH = 7
SNAPSHOT = [
    # chunk (0,0) belongs to the node under test -> writes must be applied locally
    {"d": "minecraft:overworld", "x": 0, "z": 0, "owner": "node-a"},
    # chunk (1,0) belongs to node-b -> writes must be relayed to node-b
    {"d": "minecraft:overworld", "x": 1, "z": 0, "owner": "node-b"},
]


def log(line):
    stamp = time.strftime("%H:%M:%S")
    with open(LOG, "a", encoding="utf-8") as handle:
        handle.write(f"[{stamp}] {line}\n")
    print(f"[{stamp}] {line}", flush=True)


def read_frame(sock):
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk
    length = struct.unpack(">i", header)[0]
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            return None
        body += chunk
    return json.loads(body.decode("utf-8"))


def send_frame(sock, payload):
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sock.sendall(struct.pack(">i", len(body)) + body)


CONNECTIONS = []


def control_server(port):
    """Accepts one JSON frame per line on the control port and broadcasts it to every node."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(4)
    log(f"control channel listening on 127.0.0.1:{port}")
    while True:
        conn, _ = server.accept()
        data = b""
        while not data.endswith(b"\n"):
            chunk = conn.recv(65536)
            if not chunk:
                break
            data += chunk
        if data.strip():
            frame = json.loads(data.decode("utf-8"))
            for node in list(CONNECTIONS):
                try:
                    send_frame(node, frame)
                    log(f"pushed {frame.get('type')} to node")
                except Exception as error:  # noqa: BLE001 - test stub
                    log(f"push failed: {error!r}")
        conn.close()


def handle(conn, addr):
    log(f"connection from {addr}")
    CONNECTIONS.append(conn)
    try:
        while True:
            frame = read_frame(conn)
            if frame is None:
                log("peer closed")
                return
            kind = frame.get("type")
            log(f"recv {kind}: {json.dumps(frame)[:400]}")
            if kind == "hello":
                if frame.get("secret") != "testsecret":
                    send_frame(conn, {"type": "reject", "reason": "bad secret", "retryable": False})
                    log("sent reject (bad secret)")
                    return
                send_frame(conn, {
                    "type": "hello_ack", "master": "fake-master", "session": "session-1",
                    "protocol": 1, "epoch": EPOCH, "peers": [],
                })
                log("sent hello_ack")
                send_frame(conn, {"type": "ownership_snapshot", "epoch": EPOCH, "entries": SNAPSHOT})
                log(f"sent ownership_snapshot ({len(SNAPSHOT)} entries)")
            elif kind == "ping":
                send_frame(conn, {"type": "pong", "nonce": frame.get("nonce"),
                                  "sentAt": frame.get("sentAt")})
        
    except Exception as error:  # noqa: BLE001 - test stub
        log(f"error: {error!r}")
    finally:
        if conn in CONNECTIONS:
            CONNECTIONS.remove(conn)
        conn.close()


def main():
    open(LOG, "w", encoding="utf-8").close()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", PORT))
    server.listen(4)
    log(f"fake master listening on 127.0.0.1:{PORT}")
    threading.Thread(target=control_server, args=(PORT + 1,), daemon=True).start()
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
