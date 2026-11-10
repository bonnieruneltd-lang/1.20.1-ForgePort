package dev.forgetalker.util;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of mixins that have been applied to the running JVM.
 *
 * <h2>Operational purpose</h2>
 * A mixin that fails to apply is silent at runtime: the game starts, the mod looks healthy, but the
 * ownership guard does nothing. Each mixin class calls {@link #markApplied(String)} from its static
 * initialiser (merged into the target class's {@code <clinit>} by Mixin), which proves the mixin was
 * really merged. {@code /forgetalker status} prints the result, and a missing entry is the fastest
 * way to diagnose a broken refmap after an obfuscation change.
 *
 * <h2>Thread safety</h2>
 * The backing set is concurrent, and this class deliberately depends on nothing but the JDK so it
 * can be loaded during the static initialisation of {@code net.minecraft.world.level.Level}, very
 * early in JVM startup. Do not add logging or Minecraft imports here.
 */
public final class MixinProbe {

    private static final Set<String> APPLIED = ConcurrentHashMap.newKeySet();

    private MixinProbe() {
        // Stateless registry.
    }

    /**
     * Records that one mixin was applied to its target class.
     *
     * @param marker stable identifier, e.g. {@code "Level.setBlock"}; never {@code null}
     */
    public static void markApplied(String marker) {
        APPLIED.add(marker);
    }

    /**
     * @param marker identifier to test
     * @return {@code true} when the corresponding mixin was applied in this JVM
     */
    public static boolean isApplied(String marker) {
        return APPLIED.contains(marker);
    }

    /**
     * @return an immutable view of all applied mixin markers
     */
    public static Set<String> applied() {
        return Collections.unmodifiableSet(APPLIED);
    }
}
