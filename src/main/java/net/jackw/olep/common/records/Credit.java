package net.jackw.olep.common.records;

public enum Credit {
    GC(true), BC(false);

    Credit(boolean isGood) {
        this.isGood = isGood;
    }

    private final boolean isGood;

    public byte getByteValue() {
        return (byte) (isGood ? 1 : 0);
    }

    public static Credit fromByteValue(byte value) {
        if (value == 0) {
            return BC;
        } else {
            return GC;
        }
    }
}
