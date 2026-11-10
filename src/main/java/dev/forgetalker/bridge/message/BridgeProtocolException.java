package dev.forgetalker.bridge.message;

/**
 * Signals a malformed or truncated cluster frame.
 *
 * <h2>Operational purpose</h2>
 * Netty pipeline handlers must never let a corrupt frame kill the connection silently: a decode
 * failure is logged with the offending type and the frame is dropped, while the session survives.
 * The flag {@link #isUnknownFrameType()} distinguishes "this master speaks a newer protocol"
 * (expected during rolling upgrades, log at DEBUG) from "this frame is corrupt" (log at WARN).
 *
 * <h2>Thread safety</h2>
 * Exception instances are created on the Netty event loop and never shared; the class is immutable.
 */
public final class BridgeProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean unknownFrameType;

    /**
     * @param message         description including the offending type/field
     * @param cause           underlying parser exception, may be {@code null}
     * @param unknownFrameType {@code true} when the frame type itself is unknown
     */
    public BridgeProtocolException(String message, Throwable cause, boolean unknownFrameType) {
        super(message, cause);
        this.unknownFrameType = unknownFrameType;
    }

    /**
     * Creates a corruption error (unknown type flag {@code false}).
     *
     * @param message description of the problem
     * @return the exception, never {@code null}
     */
    public static BridgeProtocolException corrupt(String message) {
        return new BridgeProtocolException(message, null, false);
    }

    /**
     * Creates a corruption error with a cause.
     *
     * @param message description of the problem
     * @param cause   underlying exception
     * @return the exception, never {@code null}
     */
    public static BridgeProtocolException corrupt(String message, Throwable cause) {
        return new BridgeProtocolException(message, cause, false);
    }

    /**
     * Creates a "newer protocol" error that callers may ignore.
     *
     * @param type wire id that could not be resolved
     * @return the exception, never {@code null}
     */
    public static BridgeProtocolException unknownType(String type) {
        return new BridgeProtocolException("Unknown frame type '" + type + "'", null, true);
    }

    /**
     * @return {@code true} when the frame should be ignored instead of logged as an error
     */
    public boolean isUnknownFrameType() {
        return unknownFrameType;
    }
}
