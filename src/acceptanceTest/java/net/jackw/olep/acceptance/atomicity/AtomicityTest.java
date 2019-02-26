package net.jackw.olep.acceptance.atomicity;

import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.acceptance.BaseAcceptanceTest;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.utils.RandomDataGenerator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * TPC-C §3.2
 */
public class AtomicityTest extends BaseAcceptanceTest {
    /**
     * §3.2.2.1
     * Perform the Payment transaction for a randomly selected warehouse, district, and customer (by customer number as
     * specified in Clause 2.5.1.2) and verify that the records in the CUSTOMER, DISTRICT, and WAREHOUSE tables have
     * been changed appropriately.
     */
    @Test
    @SuppressWarnings("AssertionFailureIgnored")
    public void testPaymentAtomicallyUpdates() throws InterruptedException, ExecutionException {
        RandomDataGenerator rand = new RandomDataGenerator();
        int warehouseId = rand.uniform(1, getConfig().getWarehouseCount());
        int districtId = rand.uniform(1, getConfig().getDistrictsPerWarehouse());
        int customerId = rand.uniform(1, getConfig().getCustomersPerDistrict());
        int customerWarehouseId = rand.uniform(1, getConfig().getWarehouseCount());
        int customerDistrictId = rand.uniform(1, getConfig().getDistrictsPerWarehouse());
        BigDecimal amount = new BigDecimal("12.34");

        ReadOnlyKeyValueStore<DistrictSpecificKey, CustomerMutable> customerMutableStore = getWorkerApp().getStreams()
            .store(KafkaConfig.CUSTOMER_MUTABLE_STORE, QueryableStoreTypes.keyValueStore());
        // CustomerMutable is itself immutable, so we can safely store a reference to it here
        CustomerMutable oldCustomer = customerMutableStore.get(new DistrictSpecificKey(customerId, customerDistrictId, customerWarehouseId));

        SettableFuture<Void> success = SettableFuture.create();

        getDb().payment(customerId, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount).addCompleteHandler(res -> {
            try {
                assertEquals(oldCustomer.balance.subtract(amount), res.customerBalance);
                success.set(null);
            } catch (Throwable e) {
                success.setException(e);
            }
        });

        success.get();
    }

    // 3.2.2.2 (rolling back) isn't applicable
}
