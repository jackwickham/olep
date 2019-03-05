package net.jackw.olep.common.records;

import net.jackw.olep.utils.populate.PredictableCustomerFactory;
import net.openhft.chronicle.bytes.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CustomerSharedSerdeTest {
    @Test
    public void testDeserializesToSameValue() {
        CustomerShared customer = PredictableCustomerFactory.instanceFor(10, 52, 100).getCustomerShared(5);
        CustomerSharedSerde serde = CustomerSharedSerde.getInstance();
        Bytes bytes = Bytes.allocateElasticDirect();
        serde.write(bytes, customer);

        CustomerShared result = serde.read(bytes, null);

        // Checks the key only
        assertEquals(customer, result);

        // Check the fields individually
        assertEquals(customer.id, result.id);
        assertEquals(customer.districtId, result.districtId);
        assertEquals(customer.warehouseId, result.warehouseId);
        assertEquals(customer.firstName, result.firstName);
        assertEquals(customer.middleName, result.middleName);
        assertEquals(customer.lastName, result.lastName);
        assertEquals(customer.address, result.address);
        assertEquals(customer.phone, result.phone);
        assertEquals(customer.since, result.since);
        assertEquals(customer.credit, result.credit);
        assertEquals(customer.creditLimit, result.creditLimit);
        assertEquals(customer.discount, result.discount);
    }
}
