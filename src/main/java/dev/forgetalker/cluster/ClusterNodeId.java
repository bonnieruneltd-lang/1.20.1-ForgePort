package dev.forgetalker.cluster;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Immutable identifier of one Forge Talker node inside a MultiPaper cluster.
 *
 * <h2>Operational purpose</h2>
 * Every cluster message, ownership entry and deferred mutation carries a node id so that the
 * receiving node can answer "is this mine?" without consulting the master. Keeping the id in a
 * dedicated value type (instead of a bare {@link String}) prevents the classic multi-node bug of
 * comparing a display name against a transport id.
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>The id matches {@code [A-Za-z0-9_.-]{1,64}} and is stored verbatim (case sensitive).</li>
 *   <li>Ids are stable across restarts: {@link NodeIdProvider} persists them to disk, because the
 *       MultiPaper master keys chunk ownership by node id. A regenerated id would look like a new
 *       node and leave stale ownership entries behind.</li>
 * </ul>
 *
 * <h2>Thread safety</h2>
 * Records are immutable, therefore this type is unconditionally thread safe. It is read from the
 * server thread (mixin guards, event handlers) and from Netty event-loop threads (frame decoding)
 * concurrently.
 *
 * <h2>Cluster context</h2>
 * {@link #SELF_COMPARISON_HINT} documents that ownership comparisons must use
 * {@link #equals(Object)} against the runtime's own id, never {@code ==}, because ids are
 * deserialized per frame and therefore are distinct instances.
 *
 * @param value the wire-visible node id, matching {@code [A-Za-z0-9_.-]{1,64}}
 */
public record ClusterNodeId(String value) implements Comparable<ClusterNodeId> {

    /** Maximum accepted id length; mirrors the master-side validation. */
    public static final int MAX_LENGTH = 64;

    /**
     * Hint for future maintainers: never use reference equality for node ids.
     * Retained as a documented constant so the rule survives refactors.
     */
    public static final String SELF_COMPARISON_HINT = "Always compare node ids with equals(), not ==";

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9_.-]{1," + MAX_LENGTH + "}");

    /**
     * Validates and normalises a node id.
     *
     * @param value raw id from configuration, a handshake frame or the persisted node-id file
     * @return a validated id; never {@code null}
     * @throws IllegalArgumentException if {@code value} is null, blank or contains illegal characters
     * @implNote The check happens at construction time so malformed ids can never reach the wire.
     */
    public ClusterNodeId {
        Objects.requireNonNull(value, "node id must not be null");
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Illegal cluster node id '" + value + "': expected " + VALID.pattern());
        }
    }

    /**
     * Parses an untrusted node id, translating validation failures into a descriptive exception.
     *
     * @param raw untrusted text (may be {@code null})
     * @return the parsed id
     * @throws IllegalArgumentException if the text is not a legal node id
     */
    public static ClusterNodeId parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Cluster node id must not be null");
        }
        return new ClusterNodeId(raw.trim());
    }

    /**
     * Returns whether this id denotes the same node as {@code other}.
     *
     * @param other candidate id, may be {@code null}
     * @return {@code true} only when both ids are non-null and equal by value
     */
    public boolean isSameNode(ClusterNodeId other) {
        return other != null && value.equals(other.value);
    }

    /**
     * Orders node ids lexicographically so ownership snapshots can be sorted deterministically.
     * Deterministic ordering keeps log diffs and tests stable.
     *
     * @param other id to compare against, never {@code null}
     * @return negative, zero or positive per {@link String#compareTo(String)}
     */
    @Override
    public int compareTo(ClusterNodeId other) {
        return value.compareTo(other.value);
    }

    /**
     * @return the node id exactly as transmitted on the cluster bus
     */
    @Override
    public String toString() {
        return value;
    }
}
