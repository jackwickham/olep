package net.jackw.olep.utils;

import net.jackw.olep.common.records.Address;

public class CommonFieldGenerators {
    public static Address generateAddress(RandomDataGenerator rand) {
        // *_STREET_1 random a-string [10 .. 20]
        String street1 = rand.aString(10, 20);
        // *_STREET_2 random a-string [10 .. 20]
        String street2 = rand.aString(10, 20);
        // *_CITY random a-string [10 .. 20]
        String city = rand.aString(10, 20);
        // *_STATE random a-string of 2 letters
        String state = rand.aString(2, 2);
        // *_ZIP is the concatenation of a random n-string of 4 numbers, and the constant string '11111'
        String zip = rand.nString(4, 4) + "11111";

        return new Address(street1, street2, city, state, zip);
    }

    public static Address generatePredictableAddress(String type, int id) {
        String template = "%s-%s-%d";
        String street1 = String.format(template, "street1", type, id);
        String street2 = String.format(template, "street2", type, id);
        String city = String.format(template, "city", type, id);
        String state = Integer.toString(id % 100);
        String zip = Integer.toString(id % 10000) + "11111";
        return new Address(street1, street2, city, state, zip);
    }


    /**
     * Given a number between 0 and 999, each of the three syllables is determined by the corresponding digit in the
     * three digit representation of the number.
     *
     * @param seed The number between 0 and 999 (inclusive) that determines the syllables
     * @return The generated name
     */
    public static String generateLastName(int seed) {
        String[] syllables = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
        return syllables[(seed / 100) % 10] + syllables[(seed / 10) % 10] + syllables[seed % 10];
    }
}
