package net.jackw.olep.utils;

import java.math.BigDecimal;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A random generator that supports the random primitives that are required for implementing TPC-C
 *
 * The random numbers produced are generated by {@link java.util.Random}, so this class can be safely used from multiple
 * threads concurrently
 */
public class RandomDataGenerator extends Random {
    /**
     * The constant C that is used when generating NURand values must be constant across all emulated terminals, for
     * each value of A. Just using a constant for simplicity.
     */
    private final int NURAND_C = 128;

    /**
     * Create a new RandomDataGenerator
     */
    public RandomDataGenerator() {
        super();
    }

    /**
     * Create a new RandomDataGenerator with the specified random seed
     *
     * @param seed The seed to initialise the {@link Random} with
     */
    public RandomDataGenerator(long seed) {
        super(seed);
    }

    /**
     * Generate a new random integer, uniformly distributed between lowerBound (inclusive) and upperBound (inclusive)
     *
     * @param lowerBound The inclusive lower bound
     * @param upperBound The inclusive upper bound
     * @return The next random int between the lower and upper bounds (inclusive)
     */
    public int uniform(int lowerBound, int upperBound) {
        checkArgument(lowerBound <= upperBound, "Lower bound must be <= upper bound");
        if (lowerBound == upperBound) {
            return lowerBound;
        }
        return this.nextInt(upperBound - lowerBound + 1) + lowerBound;
    }

    /**
     * Generate a new random long, uniformly distributed between lowerBound (inclusive) and upperBound (inclusive)
     *
     * @param lowerBound The inclusive lower bound
     * @param upperBound The inclusive upper bound
     * @return The next random long between the lower and upper bounds (inclusive)
     */
    @SuppressWarnings("MathAbsoluteRandom")
    public long uniform(long lowerBound, long upperBound) {
        checkArgument(lowerBound <= upperBound, "Lower bound must be <= upper bound");
        if (lowerBound == upperBound) {
            return lowerBound;
        }
        long range = upperBound - lowerBound + 1;
        long maxUniformValue = (Long.MAX_VALUE / range) * range;
        long result;
        do {
            result = Math.abs(this.nextLong());
            if (result < 0) {
                // Long.MIN_VALUE doesn't abs, because there is no equivalent positive integer
                // There's only a positive 0 so far though, so count this as negative 0
                // However, based on my testing using https://stackoverflow.com/a/15237585/2826188 I don't believe it's
                // actually possible to receive Long.MIN_VALUE from java.util.Random
                result = 0;
            }
        } while (result > maxUniformValue);
        return (result % range) + lowerBound;
    }

    /**
     * Generate a new random BigInteger, uniformly distributed between lowerBound (inclusive) and upperBound (inclusive)
     *
     * The bounds provided are fixed point numbers, with `decimals` specifying the position of the point.
     *
     * @param lowerBound Lower bound * 10^decimals (inclusive)
     * @param upperBound Upper bound * 10^decimals (inclusive)
     * @param decimals The number of decimal places to include in the generated random
     * @return The next random number between the lower and upper bounds (inclusive)
     */
    public BigDecimal uniform(long lowerBound, long upperBound, int decimals) {
        checkArgument(lowerBound <= upperBound, "Lower bound must be <= upper bound");
        checkArgument(decimals >= 0, "Negative decimals aren't supported");
        checkArgument(decimals < 10, "Too many decimals");

        return new BigDecimal(uniform(lowerBound, upperBound)).movePointLeft(decimals);
    }

    /**
     * Generate a non-uniform random value, distributed according to §2.1.6 of the TPC-C spec
     *
     * NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
     *
     * @param a The per-field constant
     * @param lowerBound The inclusive lower bound
     * @param upperBound The inclusive upper bound
     * @return A non-uniform random number between the lower and upper bounds (inclusive)
     */
    public int nuRand(int a, int lowerBound, int upperBound) {
        int rand0A = uniform(0, a);
        int randXY = uniform(lowerBound, upperBound);
        return (((rand0A | randXY) + NURAND_C) % (upperBound - lowerBound + 1)) + lowerBound;
    }

    /**
     * Generate a random a-string
     *
     * a-strings are have a uniformly distributed length, and each character is randomly selected from the printable
     * ASCII characters
     *
     * @param lowerBound The inclusive lower bound of the string length
     * @param upperBound The inclusive upper bound of the string length
     * @return An a-string with length between the lower and upper bounds (inclusive)
     */
    public String aString(int lowerBound, int upperBound) {
        int len = uniform(lowerBound, upperBound);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) uniform(32, 126));
        }
        return sb.toString();
    }

    /**
     * Generate a random n-string
     *
     * n-strings are have a uniformly distributed length, and each character is a randomly selected numeric character
     *
     * @param lowerBound The inclusive lower bound of the string length
     * @param upperBound The inclusive upper bound of the string length
     * @return An a-string with length between the lower and upper bounds (inclusive)
     */
    public String nString(int lowerBound, int upperBound) {
        int len = uniform(lowerBound, upperBound);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(uniform(0, 9));
        }
        return sb.toString();
    }

    /**
     * Returns `true` with probability `percentChance`%, and `false` otherwise
     *
     * @param percentChance The percentage probability of returning `true`
     * @return The random choice
     */
    public boolean choice(int percentChance) {
        return this.nextInt(100) < percentChance;
    }
}
