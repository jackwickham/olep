package net.jackw.olep.common;

import javax.annotation.Nonnull;

public class StoreKeyMissingException extends RuntimeException {
    public final Object missingKey;

    public StoreKeyMissingException(@Nonnull Object key) {
        super("Failed to get " + key.toString());
        missingKey = key;
    }
}
