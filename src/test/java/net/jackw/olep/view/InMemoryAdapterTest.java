package net.jackw.olep.view;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.SharedCustomerStore;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.message.modification.RemoteStockModification;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.rmi.RemoteException;

import static org.junit.Assert.*;

public class InMemoryAdapterTest {
    @Mock
    private SharedCustomerStore customerImmutableStore;

    private InMemoryAdapter adapter;

    @Before
    public void instantiateAdapter() throws RemoteException {
        adapter = new InMemoryAdapter(customerImmutableStore);
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
}
