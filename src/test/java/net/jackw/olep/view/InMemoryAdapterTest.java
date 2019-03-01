package net.jackw.olep.view;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.store.SharedCustomerStore;
import net.jackw.olep.common.records.Address;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.OrderLine;
import net.jackw.olep.common.records.OrderStatusResult;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.rmi.RemoteException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InMemoryAdapterTest {
    @Mock
    private SharedCustomerStore customerImmutableStore;

    private CustomerShared customer = new CustomerShared(
        1, 1, 1, "FN", "MN", "LN",
        new Address("s1", "s2", "c", "s", "z"), "1234", 10L, Credit.GC,
        BigDecimal.TEN, BigDecimal.ZERO
    );

    private InMemoryAdapter adapter;

    @Before
    public void instantiateAdapter() throws RemoteException {
        adapter = new InMemoryAdapter(customerImmutableStore, mock(DatabaseConfig.class));
    }

    @Test
    public void testStockLevelReturnsZeroWhenNoOrders() {
        assertEquals(0, adapter.stockLevel(1, 1, 1000));
    }

    @Test
    public void testStockLevelCountsItemsLessThanThreshold() {
        for (int i = 0; i < 3; i++) {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            for (int j = 0; j < 10; j++) {
                lines.add(new OrderLineModification(
                    j, i * 10 + j, 1, 1, BigDecimal.TEN, (i+1) * 10 + j, ""
                ));
            }
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, 1, lines.build(), 1L, i
            );

            adapter.newOrder(newOrder);
        }

        // There should now be n-10 items with stock < n, for 10 < n <= 40
        assertEquals(15, adapter.stockLevel(1, 1, 25));
    }

    @Test
    public void testStockLevelCountsDistinctItems() {
        for (int i = 0; i < 2; i++) {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            for (int j = 0; j < 2; j++) {
                lines.add(new OrderLineModification(
                    j, j, 1, 1, BigDecimal.TEN, 10, ""
                ));
            }
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, 1, lines.build(), 1L, i
            );

            adapter.newOrder(newOrder);
        }

        assertEquals(2, adapter.stockLevel(1, 1, 15));
    }

    @Test
    public void testStockLevelUpdatedByOrdersToDifferentDistricts() {
        for (int i = 0; i < 3; i++) {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            for (int j = 0; j < 4; j++) {
                int stock = 10 * (i + j);
                if (j == 3) {
                    stock = 10 * (4-i);
                }
                lines.add(new OrderLineModification(
                    j, j, 1, 1, BigDecimal.TEN, stock, ""
                ));
            }
            NewOrderModification newOrder = new NewOrderModification(
                1, i+1, 1, lines.build(), 1L, i
            );

            adapter.newOrder(newOrder);
        }

        assertEquals(2, adapter.stockLevel(2, 1, 25));
    }

    @Test
    public void testStockLevelNotAffectedByOtherWarehouses() {
        for (int i = 0; i < 2; i++) {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            int stock = i == 0 ? 5 : 100;
            for (int j = 0; j < 2; j++) {
                lines.add(new OrderLineModification(
                    j, i+j, i, 1, BigDecimal.TEN, stock, ""
                ));
            }
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, i+1, lines.build(), 1L, i
            );

            adapter.newOrder(newOrder);
        }

        assertEquals(2, adapter.stockLevel(1, 1, 10));
    }

    @Test
    public void testStockLevelUpdatedByRemoteStockUpdates() {
        ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
        for (int j = 0; j < 3; j++) {
            lines.add(new OrderLineModification(
                j, j, 1, 1, BigDecimal.TEN, 10, ""
            ));
        }
        NewOrderModification newOrder = new NewOrderModification(
            1, 1, 1, lines.build(), 1L, 1
        );

        adapter.remoteStock(new RemoteStockModification(0, 1, 100));
        adapter.newOrder(newOrder);
        adapter.remoteStock(new RemoteStockModification(1, 1, 100));
        adapter.remoteStock(new RemoteStockModification(2, 2, 100));

        assertEquals(2, adapter.stockLevel(1, 1, 25));
    }

    @Test
    public void testStockLevelOnlyIncludesLatest20Orders() {
        for (int i = 0; i < 30; i++) {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            for (int j = 0; j < 5; j++) {
                lines.add(new OrderLineModification(
                    j, 10*i+j, 1, 1, BigDecimal.TEN, i, ""
                ));
            }
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, 1, lines.build(), 1L, i
            );

            adapter.newOrder(newOrder);
        }

        // The first 14 orders that are considered (i=[10..23]) should be under the threshold
        assertEquals(70, adapter.stockLevel(1, 1, 24));
    }

    @Test
    public void testStockLevelLatest20OrdersNotAffectedByOrdersToOtherDistricts() {
        for (int i = 0; i < 20; i++) {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            lines.add(new OrderLineModification(
                1, i, 1, 1, BigDecimal.TEN, 10, ""
            ));
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, 1, lines.build(), 1L, i
            );

            adapter.newOrder(newOrder);
        }
        // Remote order
        {
            ImmutableList.Builder<OrderLineModification> lines = ImmutableList.builderWithExpectedSize(10);
            lines.add(new OrderLineModification(
                1, 30, 2, 1, BigDecimal.TEN, 100, ""
            ));
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, 2, lines.build(), 1L, 50
            );

            adapter.newOrder(newOrder);
        }

        assertEquals(20, adapter.stockLevel(1, 1, 25));
    }



    @Test
    public void testOrderStatusGetsCustomersOrder() {
        when(customerImmutableStore.get(new DistrictSpecificKey(1, 1, 1))).thenReturn(customer);
        String distInfo = "distinfo";
        ImmutableList<OrderLineModification> lines = ImmutableList.of(
            new OrderLineModification(
                0, 5, 1, 1, BigDecimal.TEN, 10, distInfo
            )
        );
        NewOrderModification newOrder = new NewOrderModification(
            1, 1, 1, lines, 12L, 15
        );
        adapter.newOrder(newOrder);

        OrderStatusResult result = adapter.orderStatus(1, 1, 1);
        assertEquals(1, result.customerId);
        assertEquals(1, result.districtId);
        assertEquals(1, result.warehouseId);
        assertEquals(customer.firstName, result.firstName);
        assertEquals(customer.middleName, result.middleName);
        assertEquals(customer.lastName, result.lastName);
        assertEquals(new BigDecimal("-10.00"), result.balance);
        assertEquals(newOrder.orderId, result.latestOrderId);
        assertEquals(newOrder.date, result.latestOrderDate);
        assertNull(result.latestOrderCarrierId);

        assertThat(result.latestOrderLines, Matchers.hasSize(1));
        OrderLine line = result.latestOrderLines.get(0);
        assertEquals(0, line.lineNumber);
        assertEquals(5, line.itemId);
        assertEquals(1, line.supplyWarehouseId);
        assertNull(line.deliveryDate);
        assertEquals(1, line.quantity);
        assertEquals(BigDecimal.TEN, line.amount);
        assertEquals(distInfo, line.distInfo);
    }

    @Test
    public void testOrderStatusIsUpdatedAfterDelivery() {
        when(customerImmutableStore.get(new DistrictSpecificKey(1, 1, 1))).thenReturn(customer);

        ImmutableList<OrderLineModification> lines = ImmutableList.of(
            new OrderLineModification(
                0, 5, 1, 1, new BigDecimal("12.50"), 10, ""
            )
        );
        NewOrderModification newOrder = new NewOrderModification(
            1, 1, 1, lines, 12L, 15
        );
        adapter.newOrder(newOrder);

        DeliveryModification delivery = new DeliveryModification(
            15, 1, 1, 6, 25L, 1, new BigDecimal("12.50")
        );
        adapter.delivery(delivery);

        OrderStatusResult result = adapter.orderStatus(1, 1, 1);

        assertEquals(new BigDecimal("2.50"), result.balance);
        assertEquals(6, (int) result.latestOrderCarrierId);
        assertEquals(25L, (long) result.latestOrderLines.get(0).deliveryDate);
    }

    @Test
    public void testOrderStatusUsesLatestOrderButUpdatesBalanceForOthers() {
        when(customerImmutableStore.get(new DistrictSpecificKey(1, 1, 1))).thenReturn(customer);

        for (int i = 0; i < 2; i++) {
            ImmutableList<OrderLineModification> lines = ImmutableList.of(
                new OrderLineModification(
                    0, 5+i, 1, 1, new BigDecimal("12.50"), 10, ""
                )
            );
            NewOrderModification newOrder = new NewOrderModification(
                1, 1, 1, lines, 12L+i, 15+i
            );
            adapter.newOrder(newOrder);
        }

        // Deliver the first order
        DeliveryModification delivery = new DeliveryModification(
            15, 1, 1, 6, 25L, 1, new BigDecimal("12.50")
        );
        adapter.delivery(delivery);

        OrderStatusResult result = adapter.orderStatus(1, 1, 1);

        assertEquals(new BigDecimal("2.50"), result.balance);
        assertNull(result.latestOrderCarrierId);
        assertNull(result.latestOrderLines.get(0).deliveryDate);
        assertEquals(16, result.latestOrderId);
        assertEquals(6, result.latestOrderLines.get(0).itemId);
        assertNull(result.latestOrderLines.get(0).deliveryDate);
    }

    @Test
    public void testOrderStatusWithLastNameGetsCustomerCorrectly() {
        when(customerImmutableStore.get(new CustomerNameKey(customer.lastName, 1, 1))).thenReturn(customer);

        String distInfo = "distinfo";
        ImmutableList<OrderLineModification> lines = ImmutableList.of(
            new OrderLineModification(
                0, 5, 1, 1, BigDecimal.TEN, 10, distInfo
            )
        );
        NewOrderModification newOrder = new NewOrderModification(
            1, 1, 1, lines, 12L, 15
        );
        adapter.newOrder(newOrder);

        OrderStatusResult result = adapter.orderStatus(customer.lastName, 1, 1);
        assertEquals(1, result.customerId);
        assertEquals(1, result.districtId);
        assertEquals(1, result.warehouseId);
        assertEquals(customer.firstName, result.firstName);
        assertEquals(customer.middleName, result.middleName);
        assertEquals(customer.lastName, result.lastName);
        assertEquals(new BigDecimal("-10.00"), result.balance);
        assertEquals(newOrder.orderId, result.latestOrderId);
        assertEquals(newOrder.date, result.latestOrderDate);
        assertNull(result.latestOrderCarrierId);

        assertThat(result.latestOrderLines, Matchers.hasSize(1));
        OrderLine line = result.latestOrderLines.get(0);
        assertEquals(0, line.lineNumber);
        assertEquals(5, line.itemId);
        assertEquals(1, line.supplyWarehouseId);
        assertNull(line.deliveryDate);
        assertEquals(1, line.quantity);
        assertEquals(BigDecimal.TEN, line.amount);
        assertEquals(distInfo, line.distInfo);
    }

    @Test
    public void testOrderStatusIsNotAffectedByOtherDistricts() {
        when(customerImmutableStore.get(new DistrictSpecificKey(1, 1, 1))).thenReturn(customer);

        for (int i = 0; i < 3; i++) {
            int district = i == 1 ? 2 : 1;
            int warehouse = i == 2 ? 2 : 1;
            ImmutableList<OrderLineModification> lines = ImmutableList.of(
                new OrderLineModification(
                    0, 5, warehouse, 1, new BigDecimal("12.50"), 10, ""
                )
            );
            NewOrderModification newOrder = new NewOrderModification(
                1, district, warehouse, lines, 12L, 15 + i
            );
            adapter.newOrder(newOrder);

            DeliveryModification delivery = new DeliveryModification(
                15 + i, district, warehouse, i, 25L + i, 1, new BigDecimal("12.50")
            );
            adapter.delivery(delivery);
        }

        OrderStatusResult result = adapter.orderStatus(1, 1, 1);

        assertEquals(new BigDecimal("2.50"), result.balance);
        assertEquals(0, (int) result.latestOrderCarrierId);
        assertEquals(25L, (long) result.latestOrderLines.get(0).deliveryDate);
    }
}
