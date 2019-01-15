package net.jackw.olep.utils.populate;

import com.google.common.base.Strings;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.utils.CommonFieldGenerators;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class PredictableCustomerFactory implements CustomerFactory {
    private int nextId;
    private int warehouseId;
    private int districtId;
    private int customerNameRange;

    private static Map<DistrictSpecificKey, PredictableCustomerFactory> instances = new HashMap<>();

    private PredictableCustomerFactory(int districtId, int warehouseId, int customerNameRange) {
        nextId = 1;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerNameRange = customerNameRange;
    }

    public static PredictableCustomerFactory instanceFor(DistrictShared district, int customerNameRange) {
        return instanceFor(district.id, district.warehouseId, customerNameRange);
    }

    public static PredictableCustomerFactory instanceFor(int districtId, int warehouseId, int customerNameRange) {
        DistrictSpecificKey key = new DistrictSpecificKey(customerNameRange, districtId, warehouseId);
        if (!instances.containsKey(key)) {
            instances.put(key, new PredictableCustomerFactory(districtId, warehouseId, customerNameRange));
        }
        return instances.get(key);
    }

    @Override
    public Customer makeCustomer() {
        int id = nextId++;
        return new Customer(getCustomerShared(id), getCustomerMutable(id));
    }

    public CustomerShared getCustomerShared(int id) {
        int dId = districtId;
        int wId = warehouseId;
        String last = CommonFieldGenerators.generateLastName((id - 1) % customerNameRange);
        String middle = "OE";
        String first = String.format("%d-FIRSTNAME", id);
        Address address = CommonFieldGenerators.generatePredictableAddress("CUSTOMER", id);
        String phone = String.format("%016d", id);
        long since = 1514764800000L;
        Credit credit = (id % 10 == 0) ? Credit.BC : Credit.GC;
        BigDecimal creditLim = new BigDecimal("50000.00");
        BigDecimal discount = new BigDecimal(String.format("0.%d", id % 6));

        return new CustomerShared(id, dId, wId, first, middle, last, address, phone, since, credit, creditLim, discount);
    }

    public CustomerMutable getCustomerMutable(int id) {
        BigDecimal balance = new BigDecimal("-100");
        String data = Strings.padEnd(
            String.format("Customer %d from warehouse %d, district %d", id, warehouseId, districtId),
            500, '*'
        );
        return new CustomerMutable(balance, data);
    }
}
