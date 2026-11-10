package dev.forgetalker.cluster;

/**
 * Immutable, Minecraft-free coordinate of one chunk in one dimension.
 *
 * <h2>Operational purpose</h2>
 * The ownership registry, the cluster wire protocol and the deferred mutation queue must be able
 * to name a chunk without holding a {@code ServerLevel} or a {@code LevelChunk} reference. Doing
 * so keeps ownership bookkeeping free of world-lifetime bugs (a cached chunk object keeps a
 * dimension loaded) and makes the whole subsystem unit-testable outside a game launch.
 *
 * <h2>Representation</h2>
 * The dimension is the resource-location string as produced by
 * {@code level.dimension().location().toString()} (for example {@code minecraft:overworld}).
 * Strings are used instead of a Minecraft type so that this class stays loadable in plain JUnit
 * tests and inside the Netty pipeline, where no registry access is possible.
 *
 * <h2>Thread safety</h2>
 * Immutable record, unconditionally thread safe. It is used as a key in concurrent maps, hence the
 * generated {@code equals}/{@code hashCode} are the correctness-critical part of this type.
 *
 * @param dimension resource location of the dimension, never {@code null} or blank
 * @param chunkX    chunk X coordinate (block X &gt;&gt; 4)
 * @param chunkZ    chunk Z coordinate (block Z &gt;&gt; 4)
 */
public record ChunkCoordinate(String dimension, int chunkX, int chunkZ) {

    /**
     * Canonical constructor with validation.
     *
     * @throws IllegalArgumentException if {@code dimension} is null or blank
     * @implNote Validation is intentionally cheap: this record is created for every ownership
     *         lookup performed by the mixin guards, which run at block-update frequency.
     */
    public ChunkCoordinate {
        if (dimension == null || dimension.isBlank()) {
            throw new IllegalArgumentException("dimension must not be blank");
        }
    }

    /**
     * Creates a chunk coordinate from raw chunk coordinates.
     *
     * @param dimension dimension resource location, never {@code null}
     * @param chunkX    chunk X
     * @param chunkZ    chunk Z
     * @return the coordinate, never {@code null}
     */
    public static ChunkCoordinate of(String dimension, int chunkX, int chunkZ) {
        return new ChunkCoordinate(dimension, chunkX, chunkZ);
    }

    /**
     * Converts block coordinates to the enclosing chunk coordinate.
     *
     * @param dimension dimension resource location, never {@code null}
     * @param blockX    block X, may be negative (arithmetic shift handles negatives correctly)
     * @param blockZ    block Z, may be negative
     * @return the chunk containing the block, never {@code null}
     * @implNote {@code >> 4} is used instead of {@code / 16} because integer division truncates
     *         toward zero and would map block {@code -1} to chunk {@code 0} instead of {@code -1}.
     */
    public static ChunkCoordinate fromBlock(String dimension, int blockX, int blockZ) {
        return new ChunkCoordinate(dimension, blockX >> 4, blockZ >> 4);
    }

    /**
     * @return world-space X of the chunk's minimum corner (may be negative)
     */
    public int minBlockX() {
        return chunkX << 4;
    }

    /**
     * @return world-space Z of the chunk's minimum corner (may be negative)
     */
    public int minBlockZ() {
        return chunkZ << 4;
    }

    /**
     * @return a stable, log-friendly representation such as {@code minecraft:overworld@(3,-7)}
     */
    @Override
    public String toString() {
        return dimension + "@(" + chunkX + "," + chunkZ + ")";
    }
}
