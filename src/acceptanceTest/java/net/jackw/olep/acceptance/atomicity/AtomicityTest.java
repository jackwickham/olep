package net.jackw.olep.acceptance.atomicity;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import net.jackw.olep.acceptance.BaseAcceptanceTest;
import net.jackw.olep.utils.RandomDataGenerator;
import org.junit.Test;

import javax.annotation.Nullable;
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
    public void testPaymentAtomicallyUpdates() throws InterruptedException, ExecutionException {
        RandomDataGenerator rand = new RandomDataGenerator();
        int warehouseId = rand.uniform(1, getConfig().getWarehouseCount());
        int districtId = rand.uniform(1, getConfig().getDistrictsPerWarehouse());
        int customerId = rand.uniform(1, getConfig().getCustomersPerDistrict());
        int customerWarehouseId = rand.uniform(1, getConfig().getWarehouseCount());
        int customerDistrictId = rand.uniform(1, getConfig().getDistrictsPerWarehouse());
        BigDecimal amount = new BigDecimal("12.34");

        // Send the transaction twice, and make sure we get the correct new values the second time (meaning that the
        // first was successful)
        SettableFuture<BigDecimal> balanceAfterFirstPayment = SettableFuture.create();
        SettableFuture<Void> success = SettableFuture.create();

        getDb().payment(customerId, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount).addCompleteHandler(res -> {
            balanceAfterFirstPayment.set(res.customerBalance);
        });
        getDb().payment(customerId, districtId, warehouseId, customerDistrictId, customerWarehouseId, amount).addCompleteHandler(res -> {
            Futures.addCallback(balanceAfterFirstPayment, new FutureCallback<BigDecimal>() {
                @Override
                public void onSuccess(BigDecimal result) {
                    try {
                        assertEquals(result.add(new BigDecimal("12.34")), res.customerBalance);
                        success.set(null);
                    } catch (Exception e) {
                        success.setException(e);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    success.setException(t);
                }
            }, MoreExecutors.directExecutor());
        });

        success.get();
    }

    // 3.2.2.2 (rolling back) isn't applicable
}
