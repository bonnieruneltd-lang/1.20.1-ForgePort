package dev.forgetalker.bridge.message;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.forgetalker.bridge.message.ControlMessages.Hello;
import dev.forgetalker.bridge.message.ControlMessages.HelloAck;
import dev.forgetalker.bridge.message.ControlMessages.PeerAdvertisement;
import dev.forgetalker.bridge.message.ControlMessages.PeerEndpoint;
import dev.forgetalker.bridge.message.ControlMessages.Ping;
import dev.forgetalker.bridge.message.ControlMessages.Pong;
import dev.forgetalker.bridge.message.ControlMessages.Reject;
import dev.forgetalker.bridge.message.ControlMessages.StatusRequest;
import dev.forgetalker.bridge.message.ControlMessages.StatusResponse;
import dev.forgetalker.cluster.ChunkCoordinate;
import dev.forgetalker.cluster.ClusterNodeId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * JSON codec for {@link BridgeMessage}.
 *
 * <h2>Operational purpose</h2>
 * Converts frames to and from the bytes carried by {@code BridgeFrameCodec}. JSON was chosen over
 * protobuf because it needs no code generation, a captured frame can be read by a human (or an AI
 * agent) without tooling, and unknown fields can be ignored during rolling upgrades. The cost is
 * bandwidth; control traffic is small and a block mutation is a few hundred bytes, well below the
 * noise of chunk streaming.
 *
 * <h2>Wire layout</h2>
 * Every frame is one UTF-8 JSON object whose {@code "type"} field equals {@link MessageType#wireId()}.
 * See {@code docs/PROTOCOL.md} for a field-by-field table.
 *
 * <h2>Safety</h2>
 * {@link #MAX_FRAME_BYTES} bounds decoded payloads. No Gson reflection is used, so the codec keeps
 * working if Gson changes its record handling. Decoding never touches the world, the registry or
 * the server thread; it only allocates.
 *
 * <h2>Thread safety</h2>
 * Stateless apart from an immutable shared {@link Gson}. Callable from the Netty event loop and
 * from the server thread (commands, unit tests).
 */
public final class BridgeMessageCodec {

    /** Hard upper bound for a single frame, in bytes, including the JSON envelope. */
    public static final int MAX_FRAME_BYTES = 1024 * 1024;

    private static final Gson GSON = new Gson();

    private BridgeMessageCodec() {
        // Stateless utility.
    }

    /**
     * Encodes a frame to UTF-8 JSON bytes.
     *
     * @param message frame to encode, never {@code null}
     * @return the encoded bytes, never {@code null}
     * @throws BridgeProtocolException if the message cannot be represented (e.g. oversized payload)
     * @implNote Runs on the Netty event loop when the transport flushes its outbound queue.
     */
    public static byte[] encode(BridgeMessage message) {
        if (message == null) {
            throw BridgeProtocolException.corrupt("Cannot encode null frame");
        }
        byte[] bytes = GSON.toJson(encodeJson(message)).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_FRAME_BYTES) {
            throw BridgeProtocolException.corrupt(
                    "Encoded frame too large: " + bytes.length + " > " + MAX_FRAME_BYTES);
        }
        return bytes;
    }

    /**
     * Decodes a frame from UTF-8 JSON bytes.
     *
     * @param bytes payload of one frame (without the length prefix), never {@code null}
     * @return the decoded frame, never {@code null}
     * @throws BridgeProtocolException if the frame is malformed or uses an unknown type
     */
    public static BridgeMessage decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw BridgeProtocolException.corrupt("Empty frame");
        }
        if (bytes.length > MAX_FRAME_BYTES) {
            throw BridgeProtocolException.corrupt(
                    "Frame too large: " + bytes.length + " > " + MAX_FRAME_BYTES);
        }
        try {
            JsonObject json = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
            return decodeJson(json);
        } catch (BridgeProtocolException e) {
            throw e;
        } catch (RuntimeException e) {
            throw BridgeProtocolException.corrupt("Malformed frame: " + e.getMessage(), e);
        }
    }

    /**
     * Serialises one frame into a JSON tree. Exposed for tests and for embedding a frame inside
     * {@link MutationMessages.RoutedFrame}.
     *
     * @param message frame to serialise, never {@code null}
     * @return the JSON object, never {@code null}
     */
    public static JsonObject encodeJson(BridgeMessage message) {
        JsonObject json = new JsonObject();
        json.addProperty("type", message.type().wireId());
        if (message instanceof Hello hello) {
            json.addProperty("node", hello.nodeId());
            json.addProperty("secret", hello.clusterSecret());
            json.addProperty("modVersion", hello.modVersion());
            json.addProperty("protocol", hello.protocolVersion());
            json.addProperty("role", hello.role());
            json.addProperty("peerPort", hello.peerEndpoint());
        } else if (message instanceof HelloAck ack) {
            json.addProperty("master", ack.masterNodeId());
            json.addProperty("session", ack.sessionId());
            json.addProperty("protocol", ack.protocolVersion());
            json.addProperty("epoch", ack.ownershipEpoch());
            json.add("peers", encodePeers(ack.peers()));
        } else if (message instanceof Reject reject) {
            json.addProperty("reason", reject.reason());
            json.addProperty("retryable", reject.retryable());
        } else if (message instanceof Ping ping) {
            json.addProperty("nonce", ping.nonce());
            json.addProperty("sentAt", ping.sentAtMillis());
        } else if (message instanceof Pong pong) {
            json.addProperty("nonce", pong.nonce());
            json.addProperty("sentAt", pong.sentAtMillis());
        } else if (message instanceof PeerAdvertisement advertisement) {
            json.add("peers", encodePeers(advertisement.peers()));
        } else if (message instanceof StatusRequest request) {
            json.addProperty("requestId", request.requestId());
            json.addProperty("requester", request.requesterNodeId());
        } else if (message instanceof StatusResponse response) {
            json.addProperty("requestId", response.requestId());
            json.addProperty("node", response.nodeId());
            json.addProperty("state", response.state());
            json.addProperty("ownedChunks", response.ownedChunks());
            json.addProperty("peerCount", response.peerCount());
            json.addProperty("epoch", response.ownershipEpoch());
        } else if (message instanceof ControlMessages.Goodbye goodbye) {
            json.addProperty("reason", goodbye.reason());
        } else if (message instanceof OwnershipMessages.OwnershipSnapshot snapshot) {
            json.addProperty("epoch", snapshot.epoch());
            JsonArray entries = new JsonArray();
            for (OwnershipMessages.ChunkOwnershipEntry entry : snapshot.entries()) {
                JsonObject object = new JsonObject();
                object.addProperty("d", entry.chunk().dimension());
                object.addProperty("x", entry.chunk().chunkX());
                object.addProperty("z", entry.chunk().chunkZ());
                object.addProperty("owner", entry.owner().value());
                entries.add(object);
            }
            json.add("entries", entries);
        } else if (message instanceof OwnershipMessages.OwnershipUpdate update) {
            json.addProperty("epoch", update.epoch());
            json.add("chunk", encodeChunk(update.chunk()));
            if (update.owner() != null) {
                json.addProperty("owner", update.owner().value());
            } else {
                json.add("owner", com.google.gson.JsonNull.INSTANCE);
            }
        } else if (message instanceof OwnershipMessages.ChunkInterest interest) {
            json.add("chunk", encodeChunk(interest.chunk()));
            json.addProperty("kind", interest.kind().wireId());
        } else if (message instanceof MutationMessages.BlockMutation mutation) {
            json.add("chunk", encodeChunk(mutation.chunk()));
            json.addProperty("x", mutation.x());
            json.addProperty("y", mutation.y());
            json.addProperty("z", mutation.z());
            json.addProperty("state", mutation.blockStateId());
            json.addProperty("flags", mutation.flags());
            json.addProperty("origin", mutation.originNode());
        } else if (message instanceof MutationMessages.EntitySpawn spawn) {
            json.add("chunk", encodeChunk(spawn.chunk()));
            json.addProperty("uuid", spawn.entityUuid().toString());
            json.addProperty("entityType", spawn.entityTypeId());
            json.addProperty("x", spawn.x());
            json.addProperty("y", spawn.y());
            json.addProperty("z", spawn.z());
            json.addProperty("yaw", spawn.yaw());
            json.addProperty("pitch", spawn.pitch());
            byte[] nbt = spawn.nbt();
            if (nbt != null) {
                json.addProperty("nbt", Base64.getEncoder().encodeToString(nbt));
            }
            json.addProperty("origin", spawn.originNode());
        } else if (message instanceof MutationMessages.PayloadRelay relay) {
            json.addProperty("sender", relay.senderUuid().toString());
            json.addProperty("channel", relay.channel());
            json.addProperty("payload", Base64.getEncoder().encodeToString(relay.payload()));
            json.addProperty("hop", relay.hop());
            json.addProperty("origin", relay.originNode());
        } else if (message instanceof MutationMessages.PlayerPresence presence) {
            json.addProperty("uuid", presence.playerUuid().toString());
            json.addProperty("name", presence.playerName());
            json.addProperty("kind", presence.kind().wireId());
            json.addProperty("node", presence.nodeId());
        } else if (message instanceof MutationMessages.ClusterEvent event) {
            json.addProperty("kind", event.kind());
            json.addProperty("d", event.dimension());
            json.addProperty("x", event.x());
            json.addProperty("y", event.y());
            json.addProperty("z", event.z());
            if (event.actorUuid() != null) {
                json.addProperty("actor", event.actorUuid().toString());
            }
            json.addProperty("detail", event.detail());
            json.addProperty("ts", event.timestampMillis());
        } else if (message instanceof MutationMessages.RoutedFrame routed) {
            json.addProperty("target", routed.targetNode());
            json.add("inner", encodeJson(routed.inner()));
        } else {
            throw BridgeProtocolException.corrupt("Unhandled frame class " + message.getClass());
        }
        return json;
    }

    /**
     * Rebuilds a frame from its JSON tree.
     *
     * @param json parsed frame, never {@code null}
     * @return the frame, never {@code null}
     * @throws BridgeProtocolException for unknown types or missing/invalid fields
     */
    private static BridgeMessage decodeJson(JsonObject json) {
        String rawType = json.has("type") && !json.get("type").isJsonNull()
                ? json.get("type").getAsString() : null;
        MessageType type = MessageType.fromWireId(rawType);
        if (type == null) {
            throw BridgeProtocolException.unknownType(String.valueOf(rawType));
        }
        return switch (type) {
            case HELLO -> new Hello(reqString(json, "node"), reqString(json, "secret"),
                    reqString(json, "modVersion"), reqInt(json, "protocol"), reqString(json, "role"),
                    optInt(json, "peerPort", 0));
            case HELLO_ACK -> new HelloAck(reqString(json, "master"), reqString(json, "session"),
                    reqInt(json, "protocol"), reqLong(json, "epoch"), decodePeers(json));
            case REJECT -> new Reject(reqString(json, "reason"), reqBool(json, "retryable"));
            case PING -> new Ping(reqLong(json, "nonce"), reqLong(json, "sentAt"));
            case PONG -> new Pong(reqLong(json, "nonce"), reqLong(json, "sentAt"));
            case PEER_ADVERTISEMENT -> new PeerAdvertisement(decodePeers(json));
            case STATUS_REQUEST -> new StatusRequest(reqLong(json, "requestId"),
                    reqString(json, "requester"));
            case STATUS_RESPONSE -> new StatusResponse(reqLong(json, "requestId"), reqString(json, "node"),
                    reqString(json, "state"), reqInt(json, "ownedChunks"), reqInt(json, "peerCount"),
                    reqLong(json, "epoch"));
            case GOODBYE -> new ControlMessages.Goodbye(reqString(json, "reason"));
            case OWNERSHIP_SNAPSHOT -> new OwnershipMessages.OwnershipSnapshot(reqLong(json, "epoch"),
                    decodeEntries(json));
            case OWNERSHIP_UPDATE -> new OwnershipMessages.OwnershipUpdate(reqLong(json, "epoch"),
                    decodeChunk(json, "chunk"), decodeOptionalNode(json, "owner"));
            case CHUNK_INTEREST -> new OwnershipMessages.ChunkInterest(decodeChunk(json, "chunk"),
                    requireInterestKind(optString(json, "kind")));
            case BLOCK_MUTATION -> new MutationMessages.BlockMutation(decodeChunk(json, "chunk"),
                    reqInt(json, "x"), reqInt(json, "y"), reqInt(json, "z"), reqInt(json, "state"),
                    optInt(json, "flags", 3), reqString(json, "origin"));
            case ENTITY_SPAWN -> new MutationMessages.EntitySpawn(decodeChunk(json, "chunk"),
                    reqUuid(json, "uuid"), reqString(json, "entityType"), reqDouble(json, "x"),
                    reqDouble(json, "y"), reqDouble(json, "z"), (float) optDouble(json, "yaw", 0.0D),
                    (float) optDouble(json, "pitch", 0.0D), decodeOptionalBytes(json, "nbt"),
                    reqString(json, "origin"));
            case PAYLOAD_RELAY -> new MutationMessages.PayloadRelay(reqUuid(json, "sender"),
                    reqString(json, "channel"), decodeRequiredBytes(json, "payload"),
                    optInt(json, "hop", 0), reqString(json, "origin"));
            case PLAYER_PRESENCE -> new MutationMessages.PlayerPresence(reqUuid(json, "uuid"),
                    optStringOr(json, "name", "unknown"),
                    requirePresenceKind(optString(json, "kind")), reqString(json, "node"));
            case CLUSTER_EVENT -> new MutationMessages.ClusterEvent(reqString(json, "kind"),
                    reqString(json, "d"), optInt(json, "x", 0), optInt(json, "y", 0),
                    optInt(json, "z", 0), optUuid(json, "actor"), optStringOr(json, "detail", ""),
                    optLong(json, "ts", 0L));
            case ROUTED_FRAME -> new MutationMessages.RoutedFrame(reqString(json, "target"),
                    decodeJson(reqObject(json, "inner")));
        };
    }

    /**
     * @param chunk chunk to encode, never {@code null}
     * @return {@code {"d":dimension,"x":chunkX,"z":chunkZ}}
     */
    private static JsonObject encodeChunk(ChunkCoordinate chunk) {
        JsonObject object = new JsonObject();
        object.addProperty("d", chunk.dimension());
        object.addProperty("x", chunk.chunkX());
        object.addProperty("z", chunk.chunkZ());
        return object;
    }

    /**
     * @param peers endpoints to encode, never {@code null}
     * @return JSON array of {@code {node,host,port}} objects
     */
    private static JsonArray encodePeers(List<PeerEndpoint> peers) {
        JsonArray array = new JsonArray();
        for (PeerEndpoint peer : peers) {
            JsonObject object = new JsonObject();
            object.addProperty("node", peer.nodeId());
            object.addProperty("host", peer.host());
            object.addProperty("port", peer.port());
            array.add(object);
        }
        return array;
    }

    /**
     * Decodes the peer list of a HELLO_ACK or PEER_ADVERTISEMENT frame.
     *
     * @param json frame object
     * @return immutable peer list, never {@code null}
     */
    private static List<PeerEndpoint> decodePeers(JsonObject json) {
        JsonArray array = json.has("peers") && json.get("peers").isJsonArray()
                ? json.getAsJsonArray("peers") : new JsonArray();
        List<PeerEndpoint> peers = new ArrayList<>(array.size());
        for (JsonElement element : array) {
            JsonObject object = element.getAsJsonObject();
            peers.add(new PeerEndpoint(reqString(object, "node"), reqString(object, "host"),
                    reqInt(object, "port")));
        }
        return List.copyOf(peers);
    }

    /**
     * Decodes the ownership table of an OWNERSHIP_SNAPSHOT frame.
     *
     * @param json frame object
     * @return immutable entry list, never {@code null}
     */
    private static List<OwnershipMessages.ChunkOwnershipEntry> decodeEntries(JsonObject json) {
        JsonArray array = json.has("entries") && json.get("entries").isJsonArray()
                ? json.getAsJsonArray("entries") : new JsonArray();
        List<OwnershipMessages.ChunkOwnershipEntry> entries = new ArrayList<>(array.size());
        for (JsonElement element : array) {
            JsonObject object = element.getAsJsonObject();
            entries.add(new OwnershipMessages.ChunkOwnershipEntry(decodeChunk(object, null),
                    decodeNode(object, "owner")));
        }
        return List.copyOf(entries);
    }

    /**
     * Decodes a chunk object either from the frame itself ({@code key == null}) or from a nested key.
     *
     * @param json frame object
     * @param key  field holding the chunk object, or {@code null} when {@code json} is the chunk
     * @return the chunk coordinate, never {@code null}
     */
    private static ChunkCoordinate decodeChunk(JsonObject json, String key) {
        JsonObject object = key == null ? json : reqObject(json, key);
        return new ChunkCoordinate(reqString(object, "d"), reqInt(object, "x"), reqInt(object, "z"));
    }

    /**
     * @param json frame object
     * @param key  field holding a node id
     * @return the parsed node id, never {@code null}
     */
    private static ClusterNodeId decodeNode(JsonObject json, String key) {
        try {
            return new ClusterNodeId(reqString(json, key));
        } catch (IllegalArgumentException e) {
            throw BridgeProtocolException.corrupt("Invalid node id in field '" + key + "': " + e.getMessage(), e);
        }
    }

    /**
     * @param json frame object
     * @param key  field holding an optional node id
     * @return the parsed id, or {@code null} when the field is absent or JSON null
     */
    private static ClusterNodeId decodeOptionalNode(JsonObject json, String key) {
        String raw = optString(json, key);
        if (raw == null) {
            return null;
        }
        try {
            return new ClusterNodeId(raw);
        } catch (IllegalArgumentException e) {
            throw BridgeProtocolException.corrupt("Invalid node id in field '" + key + "': " + e.getMessage(), e);
        }
    }

    /**
     * @param wireId raw interest kind
     * @return the enum constant, never {@code null}
     * @throws BridgeProtocolException when the value is unknown
     */
    private static OwnershipMessages.InterestKind requireInterestKind(String wireId) {
        OwnershipMessages.InterestKind kind = OwnershipMessages.InterestKind.fromWireId(wireId);
        if (kind == null) {
            throw BridgeProtocolException.corrupt("Unknown chunk interest kind '" + wireId + "'");
        }
        return kind;
    }

    /**
     * @param wireId raw presence kind
     * @return the enum constant, never {@code null}
     * @throws BridgeProtocolException when the value is unknown
     */
    private static MutationMessages.PresenceKind requirePresenceKind(String wireId) {
        MutationMessages.PresenceKind kind = MutationMessages.PresenceKind.fromWireId(wireId);
        if (kind == null) {
            throw BridgeProtocolException.corrupt("Unknown presence kind '" + wireId + "'");
        }
        return kind;
    }

    /**
     * Reads a mandatory string field.
     *
     * @param json frame object
     * @param key  field name
     * @return the value, never {@code null}
     * @throws BridgeProtocolException when the field is missing or null
     */
    private static String reqString(JsonObject json, String key) {
        String value = optString(json, key);
        if (value == null) {
            throw BridgeProtocolException.corrupt("Missing required string field '" + key + "'");
        }
        return value;
    }

    /**
     * Reads an optional string field.
     *
     * @param json frame object
     * @param key  field name
     * @return the value, or {@code null} when absent/null
     */
    private static String optString(JsonObject json, String key) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            return null;
        }
        return element.getAsString();
    }

    /**
     * Reads an optional string field with a fallback.
     *
     * @param json         frame object
     * @param key          field name
     * @param defaultValue value used when the field is absent
     * @return the value or {@code defaultValue}, never {@code null}
     */
    private static String optStringOr(JsonObject json, String key, String defaultValue) {
        String value = optString(json, key);
        return value == null ? defaultValue : value;
    }

    /**
     * Reads a mandatory int field.
     *
     * @param json frame object
     * @param key  field name
     * @return the value
     * @throws BridgeProtocolException when the field is missing/not numeric
     */
    private static int reqInt(JsonObject json, String key) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            throw BridgeProtocolException.corrupt("Missing required int field '" + key + "'");
        }
        try {
            return element.getAsInt();
        } catch (RuntimeException e) {
            throw BridgeProtocolException.corrupt("Field '" + key + "' is not an int", e);
        }
    }

    /**
     * Reads an optional int field.
     *
     * @param json         frame object
     * @param key          field name
     * @param defaultValue value used when the field is absent
     * @return the value or {@code defaultValue}
     */
    private static int optInt(JsonObject json, String key, int defaultValue) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            return defaultValue;
        }
        return element.getAsInt();
    }

    /**
     * Reads a mandatory long field.
     *
     * @param json frame object
     * @param key  field name
     * @return the value
     * @throws BridgeProtocolException when the field is missing/not numeric
     */
    private static long reqLong(JsonObject json, String key) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            throw BridgeProtocolException.corrupt("Missing required long field '" + key + "'");
        }
        try {
            return element.getAsLong();
        } catch (RuntimeException e) {
            throw BridgeProtocolException.corrupt("Field '" + key + "' is not a long", e);
        }
    }

    /**
     * Reads an optional long field.
     *
     * @param json         frame object
     * @param key          field name
     * @param defaultValue value used when the field is absent
     * @return the value or {@code defaultValue}
     */
    private static long optLong(JsonObject json, String key, long defaultValue) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            return defaultValue;
        }
        return element.getAsLong();
    }

    /**
     * Reads a mandatory double field.
     *
     * @param json frame object
     * @param key  field name
     * @return the value
     * @throws BridgeProtocolException when the field is missing/not numeric
     */
    private static double reqDouble(JsonObject json, String key) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            throw BridgeProtocolException.corrupt("Missing required double field '" + key + "'");
        }
        try {
            return element.getAsDouble();
        } catch (RuntimeException e) {
            throw BridgeProtocolException.corrupt("Field '" + key + "' is not a double", e);
        }
    }

    /**
     * Reads an optional double field.
     *
     * @param json         frame object
     * @param key          field name
     * @param defaultValue value used when the field is absent
     * @return the value or {@code defaultValue}
     */
    private static double optDouble(JsonObject json, String key, double defaultValue) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            return defaultValue;
        }
        return element.getAsDouble();
    }

    /**
     * Reads a mandatory boolean field.
     *
     * @param json frame object
     * @param key  field name
     * @return the value
     * @throws BridgeProtocolException when the field is missing/not boolean
     */
    private static boolean reqBool(JsonObject json, String key) {
        JsonElement element = json.get(key);
        if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            throw BridgeProtocolException.corrupt("Missing required boolean field '" + key + "'");
        }
        return element.getAsBoolean();
    }

    /**
     * Reads a mandatory UUID field in canonical form.
     *
     * @param json frame object
     * @param key  field name
     * @return the UUID, never {@code null}
     * @throws BridgeProtocolException when the field is missing or not a UUID
     */
    private static UUID reqUuid(JsonObject json, String key) {
        String raw = reqString(json, key);
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw BridgeProtocolException.corrupt("Field '" + key + "' is not a UUID: " + raw, e);
        }
    }

    /**
     * Reads an optional UUID field.
     *
     * @param json frame object
     * @param key  field name
     * @return the UUID, or {@code null} when absent/invalid-empty
     */
    private static UUID optUuid(JsonObject json, String key) {
        String raw = optString(json, key);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw BridgeProtocolException.corrupt("Field '" + key + "' is not a UUID: " + raw, e);
        }
    }

    /**
     * Reads a mandatory nested object.
     *
     * @param json frame object
     * @param key  field name
     * @return the nested object, never {@code null}
     * @throws BridgeProtocolException when the field is missing or not an object
     */
    private static JsonObject reqObject(JsonObject json, String key) {
        JsonElement element = json.get(key);
        if (element == null || !element.isJsonObject()) {
            throw BridgeProtocolException.corrupt("Missing required object field '" + key + "'");
        }
        return element.getAsJsonObject();
    }

    /**
     * Decodes a mandatory Base64 byte array.
     *
     * @param json frame object
     * @param key  field name
     * @return the bytes, never {@code null}
     * @throws BridgeProtocolException when the field is missing or not valid Base64
     */
    private static byte[] decodeRequiredBytes(JsonObject json, String key) {
        byte[] bytes = decodeOptionalBytes(json, key);
        if (bytes == null) {
            throw BridgeProtocolException.corrupt("Missing required byte field '" + key + "'");
        }
        return bytes;
    }

    /**
     * Decodes an optional Base64 byte array.
     *
     * @param json frame object
     * @param key  field name
     * @return the bytes, or {@code null} when absent
     * @throws BridgeProtocolException when the field is present but not valid Base64
     */
    private static byte[] decodeOptionalBytes(JsonObject json, String key) {
        String raw = optString(json, key);
        if (raw == null) {
            return null;
        }
        try {
            return Base64.getDecoder().decode(raw);
        } catch (IllegalArgumentException e) {
            throw BridgeProtocolException.corrupt("Field '" + key + "' is not valid Base64", e);
        }
    }
}
