# Forge Talker - Agent Handoff

> Read this before changing code. It lists the invariants that break the cluster if violated, the
> commands that prove your change works, and the places to extend without re-deriving the design.

---

## 1. Thirty-second orientation

* **Entry point:** `dev.forgetalker.ForgeTalker` (registers config + network only).
* **Composition root:** `dev.forgetalker.ForgeTalkerRuntime` - built on `ServerStartedEvent`, torn down on
  `ServerStoppingEvent`. All mixin/event code goes through its **null-safe static helpers**.
* **Single guard definition:** `dev.forgetalker.cluster.OwnershipPolicy` (pure) wrapped by
  `ChunkOwnershipGuard` (Minecraft-aware).
* **Wire model:** sealed `BridgeMessage` + explicit JSON codec in `dev.forgetalker.bridge.message`.
* **Transport:** `MultiPaperTcpTransport` (Netty, master link + peer links) behind `ClusterTransport`.
* **Inbound frames:** `PeerMessageDispatcher` (Netty thread -> server thread, in order).

---

## 2. Invariants (do not break these)

| # | Invariant | Where enforced |
|---|---|---|
| 1 | Static runtime helpers never throw and fail **open** (ALLOW) | `ForgeTalkerRuntime` |
| 2 | Guard reads only the local ownership mirror; never a network call | `ChunkOwnershipGuard` |
| 3 | Ownership epochs are monotonic; stale frames are ignored | `ChunkOwnershipRegistry` |
| 4 | Snapshots swap atomically | `ChunkOwnershipRegistry.applySnapshot` |
| 5 | Ownership frames are applied in arrival order on the server thread | `PeerMessageDispatcher` |
| 6 | Remote applies bypass the guard via `ThreadLocal` scope | `ChunkOwnershipGuard` + dispatcher |
| 7 | Nothing world-related runs on a Netty thread | `TransportListener` contract |
| 8 | Relayed payloads never re-forward (`hopLimit` default 0) | `ModdedPayloadRelay` / dispatcher |
| 9 | `Level.addFreshEntity`/`setBlock` guards are exceptions-safe | `ForgeTalkerRuntime` wrappers |
| 10 | Wire ids and record field names are stable | `MessageType`, codec, `docs/PROTOCOL.md` |
| 11 | `EntityJoinLevelEvent` is ignored when `loadedFromDisk()` | `ForgeTalkerEventBridge` |
| 12 | Players are never relayed as entities | `OwnershipPolicy.evaluateEntityMutation` |

---

## 3. Build and run

```bash
# full build: compile + unit tests + reobfuscation + jar
./gradlew build

# tests only (pure Java, no game launch)
./gradlew test

# dev server (writes run/, needs run/eula.txt with eula=true)
./gradlew runServer

# dev client
./gradlew runClient
```

Artifacts:

* `build/libs/forgetalker-1.0.0.jar` - production jar (SRG, refmap inside, `MixinConfigs` manifest).
* `build/libs/forgetalker-1.0.0-sources.jar` - sources for review if enabled.
* `build/reports/tests/test/index.html` - unit test report.

**Offline / proxied environments:** Gradle needs a truststore containing the proxy CA. This workspace
used `-Djavax.net.ssl.trustStore=... -Djavax.net.ssl.trustStorePassword=changeit` via `JAVA_OPTS` and
`GRADLE_OPTS`. Do not commit machine-specific paths into `gradle.properties`.

---

## 4. Verification recipes

### 4.1 Does the refmap still match Minecraft?

```bash
./gradlew build
unzip -p build/libs/forgetalker-1.0.0.jar forgetalker.refmap.json
```

Every mixin class must have SRG targets (`m_xxxx_`). If a mapping is missing, the injection silently
fails in production.

### 4.2 Do the mixins apply at runtime?

Run the dev server and look for:

```
Forge Talker mixins applied: [Level.setBlock, ServerLevel.addFreshEntity, ServerGamePacketListenerImpl.handleCustomPayload]
```

Missing entries mean a Mixin target changed or the refmap is absent. `MixinProbe` markers come from the
mixin static initialisers being merged into the target classes - they cannot be faked by config.

### 4.3 Does the cluster loop work end to end?

See `docs/TESTING.md` section "End-to-end cluster smoke test". It uses `scripts/fake_master.py` and
`scripts/rcon.py`; the expected evidence is:

* node logs `CONNECTING -> CONNECTED` and `Applied ownership snapshot`,
* `/forgetalker status` reports the snapshot and mixins,
* `setblock` in a locally owned chunk succeeds,
* `setblock` in a foreign chunk prints `Could not set the block` **and** the fake master logs a
  `routed_frame`/`block_mutation`,
* a `block_mutation` pushed from the master is applied (verify with `execute if block`),
* killing the master makes the node log `DISCONNECTED` and reconnect when the master returns.

---

## 5. Where to change what

| Task | File(s) |
|---|---|
| New config option | `ForgeTalkerConfig` (+ `RuntimeSettings` if hot-path) |
| New ownership rule | `OwnershipPolicy` (+ test), then `ChunkOwnershipGuard` |
| New guard for a mutation path | new mixin in `mixin/`, register in `forgetalker.mixins.json`, add `MixinProbe` marker |
| New frame | `MessageType`, message record, codec, dispatcher, PROTOCOL.md, codec test |
| New Forge event reaction | `ForgeTalkerEventBridge` |
| New command | `ForgeTalkerCommand` |
| Transport behaviour | `MultiPaperTcpTransport` (keep the listener thread contract) |
| Status output | `ForgeTalkerRuntime.statusLines`, `ClusterMetrics.summary` |

---

## 6. Debugging playbook

| Symptom | First check |
|---|---|
| foreign writes are applied locally | `/forgetalker status`: is `cluster active: true`? Are mixins applied? |
| guard rejects everything | `assumeOwnershipWhenUnknown` and whether the snapshot arrived (`Ownership: N known chunk(s)`) |
| mutations never reach the owner | `metrics relayedBlocks`, `transport pending`, peer link state, master log |
| duplicate entities | `EntitySpawn` dedupe by UUID, `loadedFromDisk` skip, entity NBT loss |
| ghost blocks | relay delivery re-check (`OwnershipPolicy.evaluateRelayDelivery`) and epoch handling |
| payload relay breaks a mod | add its namespace to `ModdedPayloadRelay.IGNORED_NAMESPACES`; document why |
| decode errors climbing | protocol version mismatch or oversized payload; enable DEBUG on `ForgeTalker` |

---

## 7. What is intentionally left for later

* A master adapter that speaks real MultiPaper protobuf messages (currently the Forge Talker framing is
  the contract; `ClusterTransport` is the seam).
* TLS / mTLS for the cluster bus.
* Per-entity ownership (currently derived from the containing chunk).
* Chunk-level handoff of in-flight ticking entities.
* Persisting the ownership mirror across restarts (deliberately not persisted: the master is authoritative).
