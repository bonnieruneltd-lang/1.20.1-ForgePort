# Forge Talker - Testing and Verification

Three levels of verification are used. Run them in this order when changing anything.

---

## 1. Unit tests (fast, no game launch)

```bash
./gradlew test
```

Covered:

| Test class | What it proves |
|---|---|
| `OwnershipPolicyTest` | every branch of the ALLOW/RELAY/REJECT decision table |
| `ChunkOwnershipRegistryTest` | snapshot swap, monotonic epochs, stale rejection, release, clear |
| `BridgeMessageCodecTest` | round-trip of **every** frame type, binary payloads, unknown types, oversize rejection |
| `DeferredMutationQueueTest` | FIFO order, blocked-head semantics, bounded capacity, budgets |

Expected: `BUILD SUCCESSFUL`, 35 tests, 0 failures.

---

## 2. Build verification

```bash
./gradlew build
unzip -l build/libs/forgetalker-1.0.0.jar | grep -E 'mixins.json|refmap|mods.toml'
unzip -p build/libs/forgetalker-1.0.0.jar META-INF/MANIFEST.MF | grep MixinConfigs
unzip -p build/libs/forgetalker-1.0.0.jar forgetalker.refmap.json | grep -c 'm_'
```

Expected: mixin config, refmap and expanded `mods.toml` inside the jar; `MixinConfigs: forgetalker.mixins.json`;
three SRG mappings in the refmap.

---

## 3. Dev server smoke test (proves mixins apply)

```bash
printf 'eula=true\n' > run/eula.txt
./gradlew runServer
```

Expected lines in `run/logs/latest.log`:

```
Forge Talker 1.0.0 loaded; waiting for the server to start before joining the cluster
Cluster disabled (clusterEnabled=false); Forge Talker runs in standalone mode
Forge Talker bootstrapped node '<id>' (clusterEnabled=false, ...)
Forge Talker mixins applied: [Level.setBlock, ServerLevel.addFreshEntity, ServerGamePacketListenerImpl.handleCustomPayload]
```

Run `/forgetalker status` from the console to confirm the same data interactively.

---

## 4. End-to-end cluster smoke test (verified in this repository)

Prerequisites: `python3` (no third-party packages).

### 4.1 Prepare the node

`run/world/serverconfig/forgetalker-server.toml`:

```toml
[cluster]
	clusterEnabled = true
	masterHost = "127.0.0.1"
	masterPort = 23600
	clusterSecret = "testsecret"
	nodeId = "node-a"
	peerBindPort = 0
[session]
	heartbeatIntervalSeconds = 2
	heartbeatTimeoutSeconds = 8
[guards]
	enforceBlockMutationGuard = true
	enforceEntityGuard = true
	assumeOwnershipWhenUnknown = true
[limits]
	debugLogging = true
```

`run/server.properties` must enable RCON:

```properties
enable-rcon=true
rcon.port=25575
rcon.password=test
```

### 4.2 Start the fake master, then the node

```bash
python3 scripts/fake_master.py 23600 /workspace/.logs/master.log &
./gradlew runServer &
```

The fake master owns chunk `(0,0)` for `node-a` and `(1,0)` for `node-b`, so writes to `(1,0)` must be
relayed.

### 4.3 Drive the guard

```bash
python3 scripts/rcon.py 'forceload add 0 0' 'forceload add 16 0'
python3 scripts/rcon.py 'setblock 0 0 0 minecraft:stone'
python3 scripts/rcon.py 'setblock 16 0 0 minecraft:diamond_block'
python3 scripts/rcon.py 'forgetalker status'
```

Expected:

```
$ setblock 0 0 0 minecraft:stone
Changed the block at 0, 0, 0            # locally owned chunk -> applied

$ setblock 16 0 0 minecraft:diamond_block
Could not set the block                 # foreign chunk -> cancelled locally
```

and the master log contains:

```
recv routed_frame: {"type":"routed_frame","target":"node-b","inner":{"type":"block_mutation",
  "chunk":{"d":"minecraft:overworld","x":1,"z":0},"x":16,"y":0,"z":0,"state":4276,"flags":2,"origin":"node-a"}}
```

### 4.4 Verify the inbound apply path

Push a mutation from the master to the node (chunk `(0,0)` is owned by `node-a`):

```bash
python3 - <<'PY'
import json, socket
frame = {"type": "block_mutation", "chunk": {"d": "minecraft:overworld", "x": 0, "z": 0},
         "x": 1, "y": 0, "z": 1, "state": 1, "flags": 3, "origin": "node-b"}
s = socket.create_connection(("127.0.0.1", 23601), timeout=5)
s.sendall((json.dumps(frame) + "\n").encode())
s.close()
PY
python3 scripts/rcon.py 'execute if block 1 0 1 minecraft:stone'
```

Expected: `Test passed`.

### 4.5 Verify reconnect

Kill the fake master and restart it. Expected node logs:

```
Cluster transport state: CONNECTED -> DISCONNECTED (master link closed)
Cluster transport state: DISCONNECTED -> CONNECTING (dial 127.0.0.1:23600)
Cluster transport state: CONNECTING -> CONNECTED (session session-1 master=fake-master epoch=7)
Applied ownership snapshot epoch 7 with 2 entries
```

Observed during development: the node reconnected after 2 failed attempts (backoff), proving that an
unreachable master does not crash or wedge the server.

---

## 5. Results recorded for this revision

| Check | Result |
|---|---|
| `./gradlew build` | SUCCESS |
| `./gradlew test` | 35 tests, 0 failures |
| jar contains refmap + mixin config + manifest attribute | yes |
| dev server starts with mod | `Done (6.975s)` |
| all three mixins applied | `[Level.setBlock, ServerLevel.addFreshEntity, ServerGamePacketListenerImpl.handleCustomPayload]` |
| master handshake + snapshot | `CONNECTED` + `Applied ownership snapshot epoch 7 with 2 entries` |
| local write allowed | `Changed the block at 0, 0, 0` |
| foreign write cancelled + relayed | `Could not set the block` + `routed_frame` in master log |
| relayed write applied on owner | `execute if block 1 0 1 minecraft:stone` -> `Test passed` |
| graceful shutdown | `goodbye: server stopping` in master log |
