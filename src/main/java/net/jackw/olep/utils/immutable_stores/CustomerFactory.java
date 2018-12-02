package net.jackw.olep.utils.immutable_stores;

import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CustomerFactory {
    private int nextId;
    private RandomDataGenerator rand;
    private int warehouseId;
    private int districtId;

    private static Map<DistrictShared.Key, CustomerFactory> instances = new HashMap<>();

    private CustomerFactory(DistrictShared district) {
        nextId = 1;
        rand = new RandomDataGenerator();
        warehouseId = district.wId;
        districtId = district.id;
    }

    public static CustomerFactory instanceFor(DistrictShared district) {
        if (!instances.containsKey(district.getKey())) {
            instances.put(district.getKey(), new CustomerFactory(district));
        }
        return instances.get(district.getKey());
    }

    /**
     * Make a new customer, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    public CustomerShared makeCustomerShared() {
        // C_ID unique within [3,000]
        int id = nextId++;
        // C_D_ID = D_ID
        int dId = districtId;
        // C_W_ID = D_W_ID
        int wId = warehouseId;
        // C_LAST generated using random syllables, seeded by iterating through the range of [0 .. 999] for the first
        // 1,000 customers, and generating a non-uniform random number using the function NURand(255, 0, 999) for each
        // of the remaining 2,000 customers
        String last = generateLast(id <= 1000 ? id -1 : rand.nuRand(255, 0, 999));
        // C_MIDDLE = "OE"
        String middle = "OE";
        // C_FIRST random a-string [8 .. 16]
        String first = rand.aString(8, 16);
        // C_STREET_1 random a-string [10 .. 20]
        String street1 = rand.aString(10, 20);
        // C_STREET_2 random a-string [10 .. 20]
        String street2 = rand.aString(10, 20);
        // C_CITY random a-string [10 .. 20]
        String city = rand.aString(10, 20);
        // C_STATE random a-string of 2 letters
        String state = rand.aString(2, 2);
        // C_ZIP is the concatenation of a random n-string of 4 numbers, and the constant string '11111'
        String zip = rand.nString(4, 4) + "11111";
        // C_PHONE random n-string of 16 numbers
        String phone = rand.nString(16, 16);
        // C_SINCE date/time given by the operating system when the CUSTOMER table was populated
        long since = new Date().getTime();
        // C_CREDIT = "GC". For 10% of the rows, selected at random, C_CREDIT = "BC"
        CustomerShared.Credit credit = rand.choice(90) ? CustomerShared.Credit.GC : CustomerShared.Credit.BC;
        // C_CREDIT_LIM = 50,000.00
        BigDecimal creditLim = new BigDecimal("50000.00");
        // C_DISCOUNT random within [0.0000 .. 0.5000]
        BigDecimal discount = rand.uniform(0L, 5000L, 4);
        // balance, ytd_payment, payment_cnt, delivery_cnt and data are not included in the shared object

        return new CustomerShared(id, dId, wId, first, middle, last, street1, street2, city, state, zip, phone, since, credit, creditLim, discount);
    }

    /**
     * Given a number between 0 and 999, each of the three syllables is determined by the corresponding digit in the
     * three digit representation of the number.
     *
     * @param seed The number between 0 and 999 (inclusive) that determines the syllables
     * @return The generated name
     */
    private String generateLast(int seed) {
        String[] syllables = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
        return syllables[(seed / 100) % 10] + syllables[(seed / 10) % 10] + syllables[seed % 10];
    }
}
