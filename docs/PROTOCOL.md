# Forge Talker - Cluster Protocol (version 1)

> Implementation: `dev.forgetalker.bridge.message` (model + codec) and `dev.forgetalker.bridge.net` (framing).
> Master side is not part of this repository; this document is the contract it must implement.

---

## 1. Transport

* TCP, one connection per direction of a node pair.
* Every frame is **4-byte big-endian length** followed by **UTF-8 JSON** of that length.
* Maximum JSON payload: `BridgeMessageCodec.MAX_FRAME_BYTES` = 1 MiB. Larger frames are dropped.
* No TLS in v1. Deploy on a private network or tunnel; the handshake secret is the only authentication.
* Heartbeats: `ping` every `heartbeatIntervalSeconds`; a master silent for `heartbeatTimeoutSeconds`
  causes the node to drop the session and reconnect with exponential backoff (1x..8x base delay,
  capped at 8x).

### Connection roles

| Role | Direction | Handshake | Carries |
|---|---|---|---|
| master link | node -> master | node sends `hello`, master replies `hello_ack` | ownership, heartbeat, routed frames, broadcasts |
| peer link | node <-> node | initiator sends `hello` with `role=peer`, listener replies `hello_ack` | block mutations, entity spawns, payload relays |

If a peer link cannot be established, `sendTo` wraps the frame in `routed_frame` and the master forwards
it to the target node.

---

## 2. Common envelope

Every frame is a JSON object with a mandatory `type` (string, see table). Unknown types are ignored
(logged at DEBUG) so a rolling upgrade does not break older nodes.

---

## 3. Frame reference

### 3.1 Control plane

| Type | Fields | Sent by | Purpose |
|---|---|---|---|
| `hello` | `node`, `secret`, `modVersion`, `protocol`, `role`, `peerPort` | node | open a session |
| `hello_ack` | `master`, `session`, `protocol`, `epoch`, `peers[{node,host,port}]` | master/peer | accept a session |
| `reject` | `reason`, `retryable` | master | refuse a session |
| `ping` | `nonce`, `sentAt` | both | liveness |
| `pong` | `nonce`, `sentAt` | both | liveness reply |
| `peer_advertisement` | `peers[{node,host,port}]` | master | direct peer endpoints |
| `status_request` | `requestId`, `requester` | node | ask a peer for status |
| `status_response` | `requestId`, `node`, `state`, `ownedChunks`, `peerCount`, `epoch` | peer | status answer |
| `goodbye` | `reason` | both | orderly close |

Example:

```json
{"type":"hello","node":"node-a","secret":"testsecret","modVersion":"1.0.0","protocol":1,"role":"server","peerPort":0}
{"type":"hello_ack","master":"fake-master","session":"session-1","protocol":1,"epoch":7,"peers":[]}
```

### 3.2 Ownership plane

| Type | Fields | Direction | Purpose |
|---|---|---|---|
| `ownership_snapshot` | `epoch`, `entries[{d,x,z,owner}]` | master -> node | full table |
| `ownership_update` | `epoch`, `chunk{d,x,z}`, `owner` (null = release) | master -> node | delta |
| `chunk_interest` | `chunk{d,x,z}`, `kind` (`claim`/`release`) | node -> master | hint |

Chunk coordinates are chunk-space (block >> 4), dimensions are resource locations
(`minecraft:overworld`). Negative coordinates are legal and encoded as JSON numbers.

Example:

```json
{"type":"ownership_snapshot","epoch":7,"entries":[{"d":"minecraft:overworld","x":0,"z":0,"owner":"node-a"},{"d":"minecraft:overworld","x":1,"z":0,"owner":"node-b"}]}
{"type":"ownership_update","epoch":8,"chunk":{"d":"minecraft:overworld","x":1,"z":0},"owner":null}
```

### 3.3 Data plane

| Type | Fields | Purpose |
|---|---|---|
| `block_mutation` | `chunk{d,x,z}`, `x`,`y`,`z`, `state`, `flags`, `origin` | apply a block write on the owner |
| `entity_spawn` | `chunk`, `uuid`, `entityType`, `x`,`y`,`z`, `yaw`,`pitch`, `nbt` (Base64), `origin` | spawn/transfer an entity |
| `payload_relay` | `sender`, `channel`, `payload` (Base64), `hop`, `origin` | replay a mod payload |
| `player_presence` | `uuid`, `name`, `kind` (`join`/`leave`), `node` | cross-node player tracking |
| `cluster_event` | `kind`, `d`, `x`,`y`,`z`, `actor`, `detail`, `ts` | informational event |
| `routed_frame` | `target`, `inner{...}` | master relay for nodes without a peer link |

Constraints enforced by the codec/records (a violation is a corrupt frame):

* `block_mutation.state` is a non-negative global `BlockState` id from `Block.BLOCK_STATE_REGISTRY`;
  every node must run the same mod set.
* `block_mutation` coordinates must fall inside the declared chunk.
* `payload_relay.payload` is Base64; the node enforces `maxRelayedPayloadBytes` (default 32768).
* `routed_frame` must not contain another `routed_frame`.
* `status_request.requester` is mandatory because the transport envelope does not expose the remote id.

Example (captured from a live run, `setblock 16 0 0 minecraft:diamond_block` on a node that does not
own chunk 1,0):

```json
{"type":"routed_frame","target":"node-b","inner":{"type":"block_mutation","chunk":{"d":"minecraft:overworld","x":1,"z":0},"x":16,"y":0,"z":0,"state":4276,"flags":2,"origin":"node-a"}}
```

---

## 4. Session sequence

```
node                                          master
 | -- tcp connect ---------------------------> |
 | -- hello(secret, protocol, peerPort) -----> |
 | <-- hello_ack(epoch, peers) --------------- |   (or reject(reason, retryable))
 | <-- ownership_snapshot(epoch) ------------- |
 | -- chunk_interest(claim) -----------------> |   as chunks load
 | -- ping ----------------------------------> |
 | <-- pong ---------------------------------- |
 | -- goodbye("server stopping") -------------> |
```

Ordering guarantees the node relies on:

1. `hello_ack` precedes `ownership_snapshot` on the same connection.
2. Ownership frames are processed in arrival order on the server thread.
3. `epoch` is monotonically increasing per master instance; the node ignores lower epochs.

---

## 5. Versioning policy

* `protocol` is compared exactly in the handshake; a mismatch is a non-retryable `reject`.
* Wire ids (`type` values) are permanent. Add, never rename.
* New optional fields are ignored by old decoders (explicit field-by-field codec).
* New frame types are dropped by old nodes with a DEBUG message.

---

## 6. Security notes

* The cluster secret is compared in constant time (`MessageDigest.isEqual`).
* The secret is never logged; `hello` is the only frame that carries it.
* Frame payloads are size-capped before parsing (1 MiB) and the Base64 payload is capped again at the
  application layer (32 KiB by default).
* A malicious peer can still forge ownership-dependent frames; the master must only advertise trusted
  peers and the network must be private.
