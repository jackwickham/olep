package net.jackw.olep.utils.immutable_stores;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.modification.DeliveryModification;
import net.jackw.olep.message.modification.NewOrderModification;
import net.jackw.olep.message.modification.OrderLineModification;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class PredictableOrderFactory implements OrderFactory {
    private int nextId;
    private int warehouseId;
    private int districtId;

    private static Map<WarehouseSpecificKey, PredictableOrderFactory> instances = new HashMap<>();

    private PredictableOrderFactory(int districtId, int warehouseId) {
        nextId = 1;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
    }

    public static PredictableOrderFactory instanceFor(DistrictShared district) {
        return instanceFor(district.id, district.warehouseId);
    }

    public static PredictableOrderFactory instanceFor(int districtId, int warehouseId) {
        WarehouseSpecificKey key = new WarehouseSpecificKey(districtId, warehouseId);
        if (!instances.containsKey(key)) {
            instances.put(key, new PredictableOrderFactory(districtId, warehouseId));
        }
        return instances.get(key);
    }

    @Override
    public DeliveredOrder makeDeliveredOrder(int customerId, StockProvider stockProvider) {
        int id = nextId++;
        return new DeliveredOrder(
            getNewOrderModification(id, customerId, false, stockProvider),
            getDeliveryModification(id, customerId)
        );
    }

    @Override
    public UndeliveredOrder makeUndeliveredOrder(int customerId, StockProvider stockProvider) {
        int id = nextId++;
        return new UndeliveredOrder(
            getNewOrderModification(id, customerId, true, stockProvider),
            getNewOrder(id, customerId)
        );
    }


    public NewOrderModification getNewOrderModification(int id, int customerId, boolean includeAmount, StockProvider stockProvider) {
        long entryDate = 1546300800000L + id * 1000;
        int lineCount = 5 + (id % 11);

        ImmutableList.Builder<OrderLineModification> lineBuilder = ImmutableList.builder();
        for (int i = 0; i < lineCount; i++) {
            lineBuilder.add(getOrderLine(id, i, includeAmount, stockProvider));
        }

        return new NewOrderModification(customerId, districtId, warehouseId, lineBuilder.build(), entryDate, id);
    }

    public OrderLineModification getOrderLine(int orderId, int lineNumber, boolean includeAmount, StockProvider stockProvider) {
        int itemId = orderId * 10 + lineNumber;
        int quantity = 5;
        BigDecimal amount;
        if (includeAmount) {
            amount = new BigDecimal(orderId).add(new BigDecimal(lineNumber).movePointLeft(2));
        } else {
            amount = BigDecimal.ZERO;
        }
        String distInfo = Strings.padEnd(String.format("%d.%d.%d.%d", warehouseId, districtId, orderId, lineNumber), 24, '*');

        return new OrderLineModification(lineNumber, itemId, warehouseId, quantity, amount, stockProvider.getStock(itemId), distInfo);
    }

    public DeliveryModification getDeliveryModification(int orderId, int customerId) {
        BigDecimal totalAmount = totalAmount(orderId);

        int carrierId = (orderId % 10) + 1;
        long deliveryDate = 1546300800000L + orderId * 1000;

        return new DeliveryModification(orderId, districtId, warehouseId, carrierId, deliveryDate, customerId, totalAmount);
    }

    public NewOrder getNewOrder(int orderId, int customerId) {
        BigDecimal totalAmount = totalAmount(orderId);

        return new NewOrder(orderId, districtId, warehouseId, customerId, totalAmount);
    }

    private BigDecimal totalAmount(int orderId) {
        int lineCount = 5 + (orderId % 11);
        BigDecimal amount = new BigDecimal(orderId).multiply(new BigDecimal(lineCount));
        return amount.add(new BigDecimal(lineCount * (lineCount + 1) / 2).movePointLeft(2));
    }
}
