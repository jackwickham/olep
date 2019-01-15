package net.jackw.olep.utils.immutable_stores;

import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;
import net.jackw.olep.utils.RandomDataGenerator;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class RandomOrderFactory implements OrderFactory {
    private RandomDataGenerator rand;
    private int nextId;
    private int warehouseId;
    private int districtId;
    private int numItems;

    private static Map<WarehouseSpecificKey, RandomOrderFactory> instances = new HashMap<>();

    private RandomOrderFactory(int districtId, int warehouseId, int numItems) {
        nextId = 1;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.numItems = numItems;
        rand = new RandomDataGenerator();
    }

    public static RandomOrderFactory instanceFor(DistrictShared district, int numItems) {
        return instanceFor(district.id, district.warehouseId, numItems);
    }

    public static RandomOrderFactory instanceFor(int districtId, int warehouseId, int numItems) {
        WarehouseSpecificKey key = new WarehouseSpecificKey(districtId, warehouseId);
        if (!instances.containsKey(key)) {
            instances.put(key, new RandomOrderFactory(districtId, warehouseId, numItems));
        }
        return instances.get(key);
    }

    @Override
    public DeliveredOrder makeDeliveredOrder(int customerId, StockProvider stockProvider) {
        int id = nextId++;

        NewOrderModification newOrderModification = makeNewOrderModification(id, customerId, false, stockProvider);
        DeliveryModification deliveryModification = makeDeliveryModification(newOrderModification);
        return new DeliveredOrder(newOrderModification, deliveryModification);
    }

    @Override
    public UndeliveredOrder makeUndeliveredOrder(int customerId, StockProvider stockProvider) {
        // O_ID unique within [3,000]
        int id = nextId++;

        NewOrderModification newOrderModification = makeNewOrderModification(id, customerId, true, stockProvider);
        NewOrder newOrder = makeNewOrder(newOrderModification);
        return new UndeliveredOrder(newOrderModification, newOrder);
    }

    private NewOrderModification makeNewOrderModification(int id, int customerId, boolean includeAmount, StockProvider stockProvider) {
        // O_ENTRY_D current date/time given by the operating system
        long entryDate = new Date().getTime();
        // O_OL_CNT random within [5 .. 15]
        int lineCount = rand.uniform(5, 15);

        ImmutableList.Builder<OrderLineModification> lineBuilder = ImmutableList.builder();
        for (int i = 0; i < lineCount; i++) {
            lineBuilder.add(makeOrderLine(i, includeAmount, stockProvider));
        }

        return new NewOrderModification(customerId, districtId, warehouseId, lineBuilder.build(), entryDate, id);
    }

    private OrderLineModification makeOrderLine(int lineNumber, boolean includeAmount, StockProvider stockProvider) {
        // OL_I_ID random within [1 .. 100,000]
        int itemId = rand.uniform(1, numItems);
        // OL_QUANTITY = 5
        int quantity = 5;
        // OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
        BigDecimal amount;
        if (includeAmount) {
            amount = rand.uniform(1, 999999, 2);
        } else {
            amount = BigDecimal.ZERO;
        }
        // OL_DIST_INFO random a-string of 24 letters
        String distInfo = rand.aString(24, 24);

        int homeWarehouseStockLevel = stockProvider.getStock(itemId);

        return new OrderLineModification(lineNumber, itemId, warehouseId, quantity, amount, homeWarehouseStockLevel, distInfo);
    }

    private DeliveryModification makeDeliveryModification(NewOrderModification o) {
        BigDecimal totalAmount = totalAmount(o);
        // O_CARRIER_ID random within [1 .. 10] if O_ID < 2,101, null otherwise
        int carrierId = rand.uniform(1, 10);
        // OL_DELIVERY_D = O_ENTRY_D if OL_O_ID < 2,101, null otherwise
        long deliveryDate = o.date;

        return new DeliveryModification(o.orderId, o.districtId, o.warehouseId, carrierId, deliveryDate, o.customerId, totalAmount);
    }

    private NewOrder makeNewOrder(NewOrderModification o) {
        BigDecimal totalAmount = totalAmount(o);
        return new NewOrder(o.orderId, o.districtId, o.warehouseId, o.customerId, totalAmount);
    }

    private BigDecimal totalAmount(NewOrderModification o) {
        BigDecimal totalAmount = BigDecimal.ZERO;
        for (OrderLineModification line : o.lines) {
            totalAmount = totalAmount.add(line.amount);
        }
        return totalAmount;
    }
}
