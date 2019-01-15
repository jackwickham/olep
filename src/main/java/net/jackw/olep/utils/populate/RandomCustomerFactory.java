package net.jackw.olep.utils.populate;

import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.utils.CommonFieldGenerators;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class RandomCustomerFactory implements CustomerFactory {
    private int nextId;
    private RandomDataGenerator rand;
    private int warehouseId;
    private int districtId;
    /**
     * The number of distinct surnames to generate
     */
    private int customerNameRange;

    private static Map<DistrictSpecificKey, RandomCustomerFactory> instances = new HashMap<>();

    private RandomCustomerFactory(DistrictShared district, int customerNameRange) {
        nextId = 1;
        rand = new RandomDataGenerator();
        warehouseId = district.warehouseId;
        districtId = district.id;
        this.customerNameRange = customerNameRange;
    }

    public static RandomCustomerFactory instanceFor(DistrictShared district, int customerNameRange) {
        DistrictSpecificKey key = new DistrictSpecificKey(customerNameRange, district.id, district.warehouseId);
        if (!instances.containsKey(key)) {
            instances.put(key, new RandomCustomerFactory(district, customerNameRange));
        }
        return instances.get(key);
    }

    @Override
    public Customer makeCustomer() {
        // C_ID unique within [3,000]
        int id = nextId++;
        return new Customer(makeCustomerShared(id), makeCustomerMutable(id));
    }

    /**
     * Make a new customer, populating fields randomly per the TPC-C spec, section 4.3.3.1
     */
    private CustomerShared makeCustomerShared(int id) {
        // C_D_ID = D_ID
        int dId = districtId;
        // C_W_ID = D_W_ID
        int wId = warehouseId;
        // C_LAST generated using random syllables, seeded by iterating through the range of [0 .. 999] for the first
        // 1,000 customers, and generating a non-uniform random number using the function NURand(255, 0, 999) for each
        // of the remaining 2,000 customers
        String last = CommonFieldGenerators.generateLastName(
            id <= customerNameRange ? id -1 : rand.nuRand(255, 0, customerNameRange - 1)
        );
        // C_MIDDLE = "OE"
        String middle = "OE";
        // C_FIRST random a-string [8 .. 16]
        String first = rand.aString(8, 16);
        Address address = CommonFieldGenerators.generateAddress(rand);
        // C_PHONE random n-string of 16 numbers
        String phone = rand.nString(16, 16);
        // C_SINCE date/time given by the operating system when the CUSTOMER table was populated
        long since = new Date().getTime();
        // C_CREDIT = "GC". For 10% of the rows, selected at random, C_CREDIT = "BC"
        Credit credit = rand.choice(90) ? Credit.GC : Credit.BC;
        // C_CREDIT_LIM = 50,000.00
        BigDecimal creditLim = new BigDecimal("50000.00");
        // C_DISCOUNT random within [0.0000 .. 0.5000]
        BigDecimal discount = rand.uniform(0L, 5000L, 4);
        // balance, ytd_payment, payment_cnt, delivery_cnt and data are not included in the shared object

        return new CustomerShared(id, dId, wId, first, middle, last, address, phone, since, credit, creditLim, discount);
    }

    private CustomerMutable makeCustomerMutable(int id) {
        // C_BALANCE = -100
        BigDecimal balance = new BigDecimal("-100");
        // C_DATA rangom a-string [300 .. 500]
        String data = rand.aString(300, 500);

        return new CustomerMutable(balance, data);
    }
}
