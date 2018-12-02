package net.jackw.olep.utils;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class RandomDataGeneratorTest {
    private RandomDataGenerator rand;

    @Before
    public void createRand() {
        rand = new RandomDataGenerator(5);
    }

    @Test
    public void testUniformIntWithinRange() {
        boolean had4 = false;
        boolean had8 = false;
        for (int i = 0; i < 30; i++) {
            int val = rand.uniform(4, 8);
            assertTrue(String.format("%d is not >= 4", val), val >= 4);
            assertTrue(String.format("%d is not <= 8", val), val <= 8);

            if (val == 4) {
                had4 = true;
            } else if (val == 8) {
                had8 = true;
            }
        }
        assertTrue(had4);
        assertTrue(had8);
    }

    @Test
    public void testUniformLongWithinRange() {
        boolean had4 = false;
        boolean had8 = false;
        for (int i = 0; i < 30; i++) {
            long val = rand.uniform(4L, 8L);
            assertTrue(String.format("%d is not >= 4", val), val >= 4L);
            assertTrue(String.format("%d is not <= 8", val), val <= 8L);

            if (val == 4L) {
                had4 = true;
            } else if (val == 8L) {
                had8 = true;
            }
        }
        assertTrue(had4);
        assertTrue(had8);
    }

    @Test
    public void testUniformBigDecimalWithinRange() {
        for (int i = 0; i < 30; i++) {
            BigDecimal val = rand.uniform(42L, 52L, 1);
            assertTrue(String.format("%s is not >= 4.2", val), val.compareTo(new BigDecimal("4.2")) >= 0);
            assertTrue(String.format("%s is not <= 5.5", val), val.compareTo(new BigDecimal("5.2")) <= 0);

            assertEquals(val.precision(), 2);
            assertEquals(val.scale(), 1);
        }
    }

    @Test
    public void testNURandWithinRange() {
        for (int i = 0; i < 30; i++) {
            int val = rand.nuRand(255, 4, 8);
            assertTrue(String.format("%d is not >= 4", val), val >= 4);
            assertTrue(String.format("%d is not <= 8", val), val <= 8);
        }
    }

    @Test
    public void testAStringValid() {
        for (int i = 0; i < 30; i++) {
            String val = rand.aString(4, 8);
            assertTrue(String.format("%s.length() is not >= 4", val), val.length() >= 4);
            assertTrue(String.format("%s.length() is not <= 8", val), val.length() <= 8);
        }
    }

    @Test
    public void testAStringWithBoundsEqual() {
        for (int i = 0; i < 10; i++) {
            String val = rand.aString(4, 4);
            assertEquals(String.format("%s.length() != 4", val), val.length(), 4);
        }
    }

    @Test
    public void testNStringValid() {
        for (int i = 0; i < 30; i++) {
            String val = rand.nString(4, 8);
            assertTrue(String.format("%s.length() is not >= 4", val), val.length() >= 4);
            assertTrue(String.format("%s.length() is not <= 8", val), val.length() <= 8);
            assertTrue("%s contains non-digit characters", val.matches("[0-9]+"));
        }
    }

    @Test
    public void testChoiceGivesCorrectDistribution() {
        int trueCount = 0;
        for (int i = 0; i < 30; i++) {
            boolean val = rand.choice(50);
            if (val) {
                trueCount++;
            }
        }
        assertTrue(trueCount >= 12);
        assertTrue(trueCount <= 18);
    }
}
