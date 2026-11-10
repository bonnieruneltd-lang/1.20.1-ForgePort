# Forge Talker - Architecture

> **Audience:** future AI coding agents and human maintainers.
> Read this file first. It is the map; every other doc is a detail view.

---

## 1. What this mod is (and is not)

Forge Talker is a **server-side compatibility bridge** that lets ordinary Forge 1.20.1 mods run on a
**MultiPaper cluster**, where each Minecraft world is hosted by several JVMs and each chunk is owned
by exactly one JVM at a time.

Forge mods assume:

* one JVM owns the whole world,
* `Level.setBlock` always succeeds locally,
* every entity in the level is ticked by this JVM,
* a serverbound mod payload is handled by the server the player is connected to.

MultiPaper breaks all four assumptions. Forge Talker closes the gap with three mechanisms:

| Mechanism | Package | Responsibility |
|---|---|---|
| Ownership mirror + guards | `dev.forgetalker.cluster`, `dev.forgetalker.mixin` | Decide whether this node may mutate a chunk/entity; relay otherwise |
| Cluster transport + protocol | `dev.forgetalker.bridge`, `dev.forgetalker.bridge.message`, `dev.forgetalker.bridge.net` | Move frames between master and peers |
| Event bridge + payload relay | `dev.forgetalker.event`, `dev.forgetalker.dispatch`, `dev.forgetalker.network` | Turn Forge events and mod payloads into cluster traffic |

It is **not** a MultiPaper implementation. MultiPaper's master and storage layer remain authoritative;
Forge Talker is a node-side client of that cluster. `MultiPaperTcpTransport` speaks the Forge Talker
cluster framing described in [PROTOCOL.md](PROTOCOL.md); a master adapter (or MultiPaper plugin) that
speaks the same framing is required. The transport is behind `ClusterTransport`, so a future agent can
swap in an adapter for a real MultiPaper master without touching guards or event handling.

---

## 2. Version matrix (do not drift)

| Component | Version | Where it is pinned |
|---|---|---|
| Minecraft | 1.20.1 | `gradle.properties` -> `minecraft_version` |
| Forge | 47.2.0 (47.x line) | `gradle.properties` -> `forge_version` |
| Java | 17 | `build.gradle` toolchain + `mixin.compatibilityLevel` |
| Mappings | official (Mojang) 1.20.1 | `gradle.properties` -> `mapping_channel/version` |
| Mixin | 0.8.5 AP / MixinGradle 0.7.38 | `build.gradle` |
| ForgeGradle | `[6.0,6.2)` | `build.gradle` |
| Cluster protocol | 1 | `ControlMessages.PROTOCOL_VERSION` |

Changing any of these requires re-validating every Mixin target (section 6) and re-running
`./gradlew build` so the refmap regenerates.

---

## 3. Module map

```
dev.forgetalker
|-- ForgeTalker                 @Mod entry point: config + network registration only
|-- ForgeTalkerRuntime          composition root + null-safe static API used by mixins/events
|
|-- config
|   |-- ForgeTalkerConfig       ForgeConfigSpec (SERVER) + reload listener
|   `-- RuntimeSettings         immutable snapshot read on hot paths (volatile swap)
|
|-- cluster                     (NO Netty, NO world mutation except the guard facade)
|   |-- ClusterNodeId           validated node id value object
|   |-- ChunkCoordinate         dimension + chunkX + chunkZ, Minecraft-free
|   |-- ChunkOwnershipRegistry  thread-safe mirror of the master's ownership table (epoch rules)
|   |-- OwnershipPolicy         pure decision table (ALLOW / RELAY / REJECT)
|   |-- MutationDecision/Verdict
|   |-- ChunkOwnershipGuard     Minecraft-aware facade + remote-apply ThreadLocal
|   `-- NodeIdProvider          stable id persisted in config/forgetalker/node-id.txt
|
|-- bridge
|   |-- ClusterTransport        SPI (start/stop/send/sendTo/state/describe/listeners)
|   |-- StandaloneTransport     inert implementation for clusterEnabled=false
|   |-- MultiPaperTcpTransport  Netty master link + peer listener + peer dialer
|   |-- ClusterBridge           facade the rest of the mod calls
|   |-- ClusterMetrics          LongAdder counters for /forgetalker status
|   |-- TransportState/Listener
|   |-- message/                sealed BridgeMessage hierarchy + JSON codec
|   `-- net/                    length-prefixed framing, encoder/decoder
|
|-- dispatch
|   |-- PeerMessageDispatcher   Netty thread -> server thread, applies frames in order
|   |-- DeferredMutationQueue   bounded FIFO for mutations whose chunk is not loaded
|   |-- PendingMutation         (chunk, frame) pair
|   `-- RelayedPayloadDispatcher  replays mod payloads through NetworkHooks
|
|-- network
|   |-- ForgeTalkerNetwork      SimpleChannel (status request/response)
|   |-- ModdedPayloadRelay      filters + forwards observed mod payloads
|   `-- packet/                 status request/response packets
|
|-- event
|   `-- ForgeTalkerEventBridge  Forge bus subscriber (lifecycle, player, block, entity, chunk, tick)
|
|-- command
|   `-- ForgeTalkerCommand      /forgetalker status|ownership|peers
|
|-- mixin                       bytecode guards (see section 6)
`-- util/MixinProbe             runtime proof that each mixin applied
```

---

## 4. Data flow

### 4.1 Startup / shutdown

```
Forge mod construction (mod bus)
        |
        +-- ForgeTalkerConfig.SPEC registered (SERVER)
        +-- ForgeTalkerNetwork.register()            SimpleChannel forgetalker:main
        +-- ForgeTalker constructor returns          (no world access)

ServerStartedEvent (server thread)
        |
        +-- ForgeTalkerRuntime.bootstrap(server)
        |     +-- ForgeTalkerConfig.active()        immutable RuntimeSettings
        |     +-- NodeIdProvider.resolve()          persisted node id
        |     +-- new ChunkOwnershipRegistry()
        |     +-- transport = clusterEnabled ? MultiPaperTcpTransport : StandaloneTransport
        |     +-- new ChunkOwnershipGuard(registry, nodeId, bridge::isClusterActive, ...)
        |     +-- new PeerMessageDispatcher(...)     registered as transport listener
        |     `-- bridge.start()                     Netty connect (async)

ServerStoppingEvent
        `-- ForgeTalkerRuntime.shutdown("server stopping")
              +-- GOODBYE frame, close channels, shutdown Netty group
              `-- registry.clear() (epoch reset)
```

### 4.2 Block mutation on a non-owning node

```
mod calls Level.setBlock(pos, state, flags)
        |
        v
LevelSetBlockMixin @HEAD  (server thread)
        |
        +-- ForgeTalkerRuntime.evaluateBlockMutation(level, pos)
        |     `-- ChunkOwnershipGuard -> OwnershipPolicy
        |           cluster inactive?        -> ALLOW   (vanilla behaviour)
        |           foreign owner known?     -> RELAY
        |           chunk not loaded?        -> REJECT
        |           owner unknown?           -> ALLOW (optimistic) / REJECT (strict)
        |           owner is this node?      -> ALLOW
        |
        +-- ALLOW  -> return, vanilla writes the block
        +-- REJECT -> cir.setReturnValue(false)               (write discarded)
        `-- RELAY  -> cir.setReturnValue(false)
                   + ForgeTalkerRuntime.relayBlockMutation(...)
                        `-- ClusterBridge.relayBlockMutation(BlockMutation)
                             +-- registry.ownerOf(chunk)
                             +-- transport.sendTo(owner, frame)
                                  peer link?  -> direct TCP peer channel
                                  otherwise   -> RoutedFrame through the master
```

On the owning node:

```
Netty thread: BridgeFrameCodec.Decoder -> BridgeMessage
        |
        v
PeerMessageDispatcher.onMessage(frame)
        `-- server.execute(() -> handleInbound(frame))     ordered, main thread
              |
              +-- OwnershipPolicy.evaluateRelayDelivery(...)  still owner?
              +-- guard.enterRemoteApply()                    ThreadLocal bypass
              +-- level.setBlock(pos, state, flags)
              `-- guard.exitRemoteApply()
```

The `ThreadLocal` bypass is required: the relayed write re-enters `Level.setBlock`, and the local
mirror may not yet have processed the ownership delta that made this node the owner. Without the
bypass the guard would see "unknown owner" and reject the very mutation it was asked to apply.

### 4.3 Modded payload relay

```
Node A (player connected)                           Node B (owns the chunk)
--------------------------                          -----------------------
ServerGamePacketListenerImplMixin @HEAD
  copies channel + bytes (non-destructive)
        |
        v
ModdedPayloadRelay.observeInboundCustomPayload
  + filter namespace (minecraft/forge/fml/forgetalker)
  + size cap (maxRelayedPayloadBytes, default 32 KiB)
        |
        v
ClusterBridge.broadcastPayload(PayloadRelay)
        |
        +------------ cluster bus (master and/or peer links) ------------>
                                                            PayloadRelay frame
                                                                   |
                                                                   v
                                                    RelayedPayloadDispatcher (main thread)
                                                      +-- for each local player:
                                                      |     new ServerboundCustomPayloadPacket(
                                                      |         channel, fresh FriendlyByteBuf)
                                                      `-- NetworkHooks.onCustomPayload(packet, conn)
                                                              => the mod's own SimpleChannel handler
```

`PAYLOAD_RELAY` carries a hop counter and `relayPayloadHopLimit` defaults to `0`, so a frame is applied
by the receiving nodes and never re-forwarded. That is the loop guard.

### 4.4 Ownership synchronisation

```
master                                    node
  |  HELLO_ACK(epoch=17, peers=[...])  --->
  |  OWNERSHIP_SNAPSHOT(epoch=17)      --->  registry.applySnapshot (atomic swap)
  |  OWNERSHIP_UPDATE(epoch=18, chunk) --->  registry.applyUpdate   (in-order delta)
  |  <--- CHUNK_INTEREST(claim/release)       from ChunkEvent.Load/Unload
  |  <--- PING / PONG                        heartbeat every heartbeatIntervalSeconds
```

Registry rules (implemented and unit-tested in `ChunkOwnershipRegistry`):

* epochs are monotonic; older snapshots/deltas are ignored,
* snapshots are swapped atomically so readers never see a partial table,
* deltas are applied in arrival order because `PeerMessageDispatcher` funnels every frame through
  `MinecraftServer.execute` and never reorders,
* `clear()` resets the epoch so a restarted master may restart numbering at 0,
* an absent entry means "unassigned", not "foreign"; `OwnershipPolicy` decides the meaning.

### 4.5 Failure / fallback model

| Situation | State | Guard behaviour |
|---|---|---|
| `clusterEnabled=false` | `STANDALONE` | everything ALLOW (vanilla) |
| master unreachable | `DISCONNECTED`, retry with backoff | everything ALLOW |
| TCP up, handshake pending | `HANDSHAKING` | everything ALLOW |
| session active | `CONNECTED` / `DEGRADED` | full ownership enforcement |
| master silent past timeout | session dropped -> `DISCONNECTED` | everything ALLOW, registry cleared |

Availability is preferred over split-brain protection **only when there is no authoritative data**.
While a snapshot is authoritative, foreign writes are never applied locally.

---

## 5. Thread model

| Execution context | Thread name | May touch world? | Enters the mod through |
|---|---|---|---|
| Server main | `Server thread` | yes | mixins, Forge events, commands, deferred drain |
| Netty I/O | `ForgeTalker-Netty` | **no** | frame decode, `TransportListener.onMessage/onStateChanged` |
| Command | server main | read-only | `/forgetalker *` |
| Config load | Forge config thread | no | `ForgeTalkerConfig.onConfigChanged` |

Rules that keep this safe:

1. **Nothing world-related runs on a Netty thread.** `PeerMessageDispatcher.onMessage` is the only
   listener in production and immediately re-dispatches through `MinecraftServer.execute`.
2. **Ownership writes and reads use one concurrent structure.** `ChunkOwnershipRegistry` publishes a
   `volatile` `ConcurrentHashMap`; snapshots are swapped atomically.
3. **Config is read through a volatile immutable record**, never through `ForgeConfigSpec` on hot paths.
4. **Remote applies are marked with a `ThreadLocal`**, which is correct because they only run on the
   server thread.
5. **Sending is enqueue-only.** `ClusterTransport.send*` must never block; `MultiPaperTcpTransport`
   writes to channels (async) or buffers in a bounded queue.
6. **The runtime is null-safe.** Mixins run before `ServerStartedEvent` and after `ServerStoppingEvent`;
   every static helper tolerates a null runtime and fails open (ALLOW).

---

## 6. Mixin injection points (and why)

All three targets were verified against the mapped 1.20.1 jar with `javap`; the descriptors below are
exactly the ones the annotation processor remapped to SRG (see `forgetalker.refmap.json`).

### 6.1 `LevelSetBlockMixin` -> `net.minecraft.world.level.Level`

* Target: `setBlock(Lnet/minecraft/core/BlockPos;Lnet/minecraft/world/level/block/state/BlockState;II)Z`
* Injection: `@At("HEAD")`, `cancellable = true`
* SRG: `m_6933_`

**Why this method:** it is the funnel for every block write in 1.20.1. The three-argument overload
delegates to it, `setBlockAndUpdate` delegates to it, `destroyBlock` delegates to it, and Forge's item
placement path ends there. Targeting the three-argument overload would miss direct four-argument calls
from mods.

**Why `Level` and not `ServerLevel`:** `ServerLevel` does not override `setBlock`; a mixin there would
never fire. The handler checks `instanceof ServerLevel` and ignores client levels and proto-chunk writes.

**Why HEAD:** the guard must prevent the write, not observe it; a `RETURN` injector would run after the
world already changed.

### 6.2 `ServerLevelEntityMixin` -> `net.minecraft.server.level.ServerLevel`

* Target: `addFreshEntity(Lnet/minecraft/world/entity/Entity;)Z`
* Injection: `@At("HEAD")`, `cancellable = true`
* SRG: `m_7967_`

**Why this method:** the single entry point for mob spawns, item drops, modded spawners and
`addFreshEntityWithPassengers`. Cancelling at HEAD means observers (`EntityJoinLevelEvent`, mod hooks)
never see an entity that would be deleted a moment later.

**Players are exempt** in `OwnershipPolicy`: a `ServerPlayer` exists only on the node it is connected
to, so relaying it would duplicate the player.

### 6.3 `ServerGamePacketListenerImplMixin` -> `net.minecraft.server.network.ServerGamePacketListenerImpl`

* Target: `handleCustomPayload(Lnet/minecraft/network/protocol/game/ServerboundCustomPayloadPacket;)V`
* Injection: `@At("HEAD")`, **not** cancellable
* SRG: `m_7423_`

**Why this method:** the only server-side handler for custom payloads, and Forge patches it to dispatch
to every registered `SimpleChannel`. Injecting here sees the raw channel name and bytes before any mod
consumes them.

**Why not cancel:** the payload must still be handled on the node the player is connected to. The mixin
is an observer; `ModdedPayloadRelay` decides whether to forward.

**Non-destructive read:** `FriendlyByteBuf.getBytes(readerIndex, ...)` copies without moving the reader
index, so Forge's own dispatch sees the same bytes.

### 6.4 Refmap and production behaviour

`MixinGradle` runs the Mixin annotation processor with ForgeGradle's SRG mappings, producing
`forgetalker.refmap.json` inside the jar. Without that file the mixins work in the dev environment
(named mappings) and silently fail in production (SRG names). `MixinProbe` exists to make that failure
visible: each mixin's static initialiser registers a marker, and `/forgetalker status` prints the set.

---

## 7. Event bridge

| Forge event | Action |
|---|---|
| `ServerStartedEvent` | bootstrap runtime, connect transport |
| `ServerStoppingEvent` | GOODBYE + shutdown |
| `PlayerEvent.PlayerLoggedInEvent` | broadcast `PLAYER_PRESENCE(join)`, CLAIM player chunk |
| `PlayerEvent.PlayerLoggedOutEvent` | broadcast `PLAYER_PRESENCE(leave)` |
| `BlockEvent.BreakEvent` | foreign chunk: cancel + relay AIR; local: broadcast `block_break` |
| `EntityJoinLevelEvent` | foreign chunk: cancel + relay spawn (skipped when `loadedFromDisk`) |
| `ChunkEvent.Load` | `CHUNK_INTEREST(claim)` |
| `ChunkEvent.Unload` | `CHUNK_INTEREST(release)` |
| `TickEvent.ServerTickEvent` (END) | drain deferred mutations, 256/tick budget |

`EntityJoinLevelEvent.loadedFromDisk()` is checked because chunk loading fires the event for every
already-existing entity; relaying those would duplicate entities cluster-wide.

---

## 8. Extension guide (for the next agent)

### Add a cluster frame

1. Add a constant to `MessageType` (never rename existing wire ids).
2. Add a record to `ControlMessages`, `OwnershipMessages` or `MutationMessages` and list it in the
   `permits` clause of `BridgeMessage`.
3. Add an `encodeJson` branch and a `decodeJson` case in `BridgeMessageCodec`.
4. Add a round-trip test in `BridgeMessageCodecTest`.
5. Handle it in `PeerMessageDispatcher.handleInbound` (if/else chain - Java 17 has no pattern switch).
6. Document it in `docs/PROTOCOL.md`.

### Add an ownership guard for a new mutation path

1. Add a pure rule to `OwnershipPolicy` (no Minecraft types) and a unit test.
2. Expose it through `ChunkOwnershipGuard`.
3. Expose a null-safe static helper on `ForgeTalkerRuntime`.
4. Add a mixin or event handler that calls the helper and cancels/relays.
5. Register the mixin in `forgetalker.mixins.json`, add a `MixinProbe.markApplied` marker, and verify
   the target with `javap` against the mapped jar.

### Replace the transport

Implement `ClusterTransport`, return it from `ForgeTalkerRuntime.bootstrap`, and keep the
`TransportListener` contract (callbacks on a non-server thread). Guards and the dispatcher need no
changes.

---

## 9. Known limitations (be explicit, do not paper over)

1. **Homogeneous mod set required.** `BlockMutation` carries the global `BlockState` id and
   `EntitySpawn` the entity type id; nodes with different blocks/mods would desync. Validate this in
   the deployment checklist.
2. **Payload replay approximates the sender.** `NetworkHooks.onCustomPayload` is invoked with a local
   player's connection, so `ctx.getSender()` is that player, not the original sender. Mods that key
   security decisions on the sender must exclude their channel (see `ModdedPayloadRelay.IGNORED_NAMESPACES`).
3. **No TLS.** The cluster bus is plain TCP with a shared secret. Run it on a private network or tunnel.
4. **Entity transfer is NBT-only.** Cross-node entity handoff recreates the entity from NBT; entity-
   bound mod capabilities that are not serialised will be lost.
5. **`Level.setBlock` is guarded, `ChunkAccess.setBlockState` is not.** Worldgen writes into proto
   chunks are intentionally untouched (they run before a chunk is assigned).
6. **Deferred queue is best-effort.** If a chunk never loads, buffered mutations are eventually
   evicted (bounded memory) and counted in `deferredDropped`.
7. **Status replies need a requester id** because the transport envelope does not expose remote
   identity; `StatusRequest.requesterNodeId` carries it explicitly.

---

## 10. Verification checklist (run before claiming success)

```bash
./gradlew build            # compiles, runs tests, reobfuscates, builds the jar
./gradlew test            # pure-Java unit tests (policy, registry, codec, queue)
unzip -p build/libs/forgetalker-1.0.0.jar forgetalker.refmap.json   # SRG mappings present
./gradlew runServer       # dev server: expect "Forge Talker mixins applied: [...]"
```

Expected server log lines:

```
Forge Talker 1.0.0 loaded; waiting for the server to start before joining the cluster
Forge Talker bootstrapped node '<id>' (clusterEnabled=false, ...)
Forge Talker mixins applied: [Level.setBlock, ServerLevel.addFreshEntity, ServerGamePacketListenerImpl.handleCustomPayload]
```
