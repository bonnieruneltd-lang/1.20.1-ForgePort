# Forge Talker

**A MultiPaper compatibility bridge for Forge 1.20.1.**
Forge Talker lets ordinary, unmodified Forge mods run on a distributed MultiPaper cluster where each
chunk is owned by exactly one node, modded payloads must reach the node that owns the world state, and
Forge server events must be visible cluster-wide.

Target: **Minecraft 1.20.1 / Forge 47.x / Java 17** only.

---

## What it does

| Problem on a MultiPaper cluster | Forge Talker mechanism |
|---|---|
| Forge mods write blocks/entities in chunks owned by another node | Mixin guards on `Level.setBlock` and `ServerLevel.addFreshEntity` + `OwnershipPolicy` (ALLOW / RELAY / REJECT) |
| The owning node never sees a custom mod payload | `SimpleChannel`/Netty observation of `handleCustomPayload`, relayed over the cluster bus and replayed through `NetworkHooks` |
| Forge events only fire on one node | `ForgeTalkerEventBridge` broadcasts player presence, block breaks and entity joins |
| Nodes disagree about ownership | Epoch-checked ownership mirror fed by a master snapshot + deltas |

Everything is documented for hand-off:

* [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) - module boundaries, data-flow diagrams, mixin rationale
* [`docs/PROTOCOL.md`](docs/PROTOCOL.md) - wire framing, every frame, ordering guarantees
* [`docs/AGENT_HANDOFF.md`](docs/AGENT_HANDOFF.md) - invariants, verification recipes, debug playbook
* [`docs/TESTING.md`](docs/TESTING.md) - unit, build and end-to-end test procedures with observed results

---

## Quick start (server operator)

1. Drop `forgetalker-1.0.0.jar` into the node's `mods/` folder (every node must have it, plus an
   identical mod set - block/entity state ids are exchanged by registry id).
2. Start the server once to generate `world/serverconfig/forgetalker-server.toml`.
3. Configure:

```toml
[cluster]
	clusterEnabled = true
	masterHost = "10.0.0.5"
	masterPort = 23545
	clusterSecret = "<shared secret>"
	nodeId = ""            # empty = generate and persist in config/forgetalker/node-id.txt
	peerBindHost = "0.0.0.0"
	peerBindPort = 23546    # 0 disables direct peer links (all traffic via the master)
```

4. Restart. Check `/forgetalker status`:

```
Node: node-a (cluster active: true)
Transport: CONNECTED master=10.0.0.5:23545 peers=1/1 pending=0 peerListener=0.0.0.0:23546
Ownership: 512 known chunk(s), epoch 41, local 128
Applied mixins: [Level.setBlock, ServerLevel.addFreshEntity, ServerGamePacketListenerImpl.handleCustomPayload]
```

If `Applied mixins` is empty or `cluster active` is false, the mod is inert by design (fail-open) -
see `docs/AGENT_HANDOFF.md` section 6.

---

## Building

```bash
./gradlew build      # jar in build/libs/forgetalker-1.0.0.jar
./gradlew test       # 35 pure-Java unit tests
./gradlew runServer  # dev server (requires run/eula.txt)
```

Requirements: JDK 17, Gradle wrapper 8.1.1 (bundled). ForgeGradle 6 downloads Minecraft 1.20.1 and
decompiles it on first run.

---

## Configuration summary

| Option | Default | Meaning |
|---|---|---|
| `cluster.clusterEnabled` | `false` | master of all switches; false = vanilla Forge behaviour |
| `cluster.masterHost` / `masterPort` | `127.0.0.1` / `23545` | MultiPaper master endpoint |
| `cluster.clusterSecret` | `""` | handshake secret (constant-time compare) |
| `cluster.nodeId` | `""` | explicit id, otherwise generated + persisted |
| `cluster.peerBindPort` | `23546` | direct peer listener, `0` = master relay only |
| `session.reconnectDelaySeconds` | `5` | base backoff (up to 8x) |
| `session.heartbeatIntervalSeconds/TimeoutSeconds` | `5` / `20` | liveness detection |
| `guards.enforceBlockMutationGuard` | `true` | mixin guard for `Level.setBlock` |
| `guards.enforceEntityGuard` | `true` | mixin guard for `ServerLevel.addFreshEntity` |
| `guards.assumeOwnershipWhenUnknown` | `true` | optimistic (true) vs strict (false) unassigned chunks |
| `payloads.relayModdedPayloads` | `true` | forward modded custom payloads |
| `payloads.maxRelayedPayloadBytes` | `32768` | size cap per relayed payload |
| `limits.deferredMutationQueueLimit` | `4096` | bounded buffer for unloaded chunks |
| `limits.debugLogging` | `false` | per-mutation debug logs |

Connection settings are read at server start; guard and relay switches are re-evaluated live from the
reloaded config snapshot.

---

## Verified in this revision

* `./gradlew build` + `./gradlew test` green (35 tests).
* Dev server boots with the mod; all three mixins report as applied at runtime.
* Against a stub master: handshake, ownership snapshot, local write allowed, foreign write cancelled
  and relayed as `block_mutation`, relayed write applied on the owning node, reconnect after master
  loss, graceful `goodbye` on shutdown. Details and commands: [`docs/TESTING.md`](docs/TESTING.md).

---

## Status and limitations

Forge Talker is a node-side bridge. It speaks the documented Forge Talker cluster framing; a master
adapter that speaks the same framing (or a MultiPaper plugin bridging to it) is required and is not
part of this repository. The cluster bus is plain TCP with a shared secret - run it on a private
network. Cross-node entity transfer is NBT-based, so non-serialised mod capabilities are lost. See
`docs/ARCHITECTURE.md` section 9 for the full list.

---

## License

MIT - see [`LICENSE`](LICENSE).
