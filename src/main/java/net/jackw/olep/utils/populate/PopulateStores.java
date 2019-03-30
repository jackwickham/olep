package net.jackw.olep.utils.populate;

import com.google.errorprone.annotations.MustBeClosed;
import net.jackw.olep.common.DatabaseConfig;
import net.jackw.olep.common.JsonSerializer;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.records.Customer;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.DistrictSpecificKeySerde;
import net.jackw.olep.common.records.NewOrder;
import net.jackw.olep.common.records.Stock;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.common.records.StockShared;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKeySerde;
import net.jackw.olep.message.modification.ModificationKey;
import net.jackw.olep.message.modification.ModificationMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("FutureReturnValueIgnored")
public class PopulateStores implements AutoCloseable {
    private boolean populateImmutableStores;
    private boolean populateMutableStores;
    private DatabaseConfig config;

    private PopulatableDatabase db;

    private ArrayList<Integer> customerIds;

    private long transactionId = 0;

    @MustBeClosed
    @SuppressWarnings("MustBeClosedChecker")
    public PopulateStores(DatabaseConfig config, boolean populateImmutableStores, boolean populateMutableStores) {
        this.populateImmutableStores = populateImmutableStores;
        this.populateMutableStores = populateMutableStores;
        this.config = config;

        db = PopulatableDatabase.getInstance(config, populateImmutableStores, populateMutableStores);

        customerIds = IntStream.range(1, config.getCustomersPerDistrict()+1).boxed().collect(Collectors.toCollection(ArrayList::new));
    }

    public void populate() {
        resetIds();
        populateItems();
        populateWarehouses();
    }


    private void resetIds() {
        PredictableCustomerFactory.resetInstances();
        PredictableDistrictFactory.resetInstances();
        PredictableItemFactory.resetInstance();
        PredictableOrderFactory.resetInstances();
        PredictableStockFactory.resetInstances();
        PredictableWarehouseFactory.resetInstance();
        RandomCustomerFactory.resetInstances();
        RandomDistrictFactory.resetInstances();
        RandomItemFactory.resetInstance();
        RandomOrderFactory.resetInstances();
        RandomStockFactory.resetInstances();
        RandomWarehouseFactory.resetInstance();
    }

    private void populateItems() {
        ItemFactory factory;
        if (config.isPredictableData()) {
            factory = PredictableItemFactory.getInstance();
        } else {
            factory = RandomItemFactory.getInstance();
        }

        for (int i = 0; i < config.getItemCount(); i++) {
            Item item = factory.makeItem();
            if (populateImmutableStores) {
                db.populateItem(item);
            }
        }
    }

    private void populateWarehouses() {
        WarehouseFactory factory;
        if (config.isPredictableData()) {
            factory = PredictableWarehouseFactory.getInstance();
        } else {
            factory = RandomWarehouseFactory.getInstance();
        }

        for (int wh = 0; wh < config.getWarehouseCount(); wh++) {
            WarehouseShared warehouse = factory.makeWarehouseShared();
            if (populateImmutableStores) {
                db.populateWarehouse(warehouse);
            }

            int[] stockQuantities = populateStock(warehouse);
            populateDistricts(warehouse, stockQuantities);
        }
    }

    private void populateDistricts(WarehouseShared warehouse, int[] stockQuantities) {
        DistrictFactory districtFactory;
        if (config.isPredictableData()) {
            districtFactory = PredictableDistrictFactory.instanceFor(warehouse);
        } else {
            districtFactory = RandomDistrictFactory.instanceFor(warehouse);
        }
        for (int dst = 0; dst < config.getDistrictsPerWarehouse(); dst++) {
            DistrictShared district = districtFactory.makeDistrictShared();
            if (populateImmutableStores) {
                db.populateDistrict(district);
            }

            populateCustomers(district);

            int nextOrderId = populateOrders(district, stockQuantities);
            db.finaliseDistrict(district, nextOrderId);
        }
    }

    private void populateCustomers(DistrictShared district) {
        CustomerFactory customerFactory;
        if (config.isPredictableData()) {
            customerFactory = PredictableCustomerFactory.instanceFor(district, config.getCustomerNameRange());
        } else {
            customerFactory = RandomCustomerFactory.instanceFor(district, config.getCustomerNameRange());
        }
        for (int cust = 0; cust < config.getCustomersPerDistrict(); cust++) {
            Customer customer = customerFactory.makeCustomer();
            db.populateCustomer(customer);
        }
    }

    private int[] populateStock(WarehouseShared warehouse) {
        StockFactory stockFactory;
        if (config.isPredictableData()) {
            stockFactory = PredictableStockFactory.instanceFor(warehouse);
        } else {
            stockFactory = RandomStockFactory.instanceFor(warehouse);
        }
        int[] stockQuantities = new int[config.getItemCount()];
        for (int item = 0; item < config.getItemCount(); item++) {
            Stock stock = stockFactory.makeStock();
            db.populateStock(stock);
            stockQuantities[item] = stock.stockQuantity;
        }
        return stockQuantities;
    }

    private int populateOrders(DistrictShared district, int[] stockQuantities) {
        OrderFactory orderFactory;
        if (config.isPredictableData()) {
            orderFactory = PredictableOrderFactory.instanceFor(district, config.getItemCount());
        } else {
            orderFactory = RandomOrderFactory.instanceFor(district, config.getItemCount());
            Collections.shuffle(customerIds);
        }

        if (populateMutableStores) {
            OrderFactory.StockProvider stockProvider = itemId -> stockQuantities[itemId - 1];
            int processedCustomers = 0;
            for (int customerId : customerIds) {
                if (config.isPredictableData() || processedCustomers++ < config.getCustomersPerDistrict() * 7 / 10) {
                    // For 7/10 of the customers, the order has been delivered already (and to simplify testing, predictable
                    // mode doesn't create any pending orders, so they have to be created manually)
                    OrderFactory.DeliveredOrder order = orderFactory.makeDeliveredOrder(customerId, stockProvider);
                    db.populateDeliveredOrder(order);
                } else {
                    // For the remaining 3/10, the order is still outstanding, so we add it as a NewOrder too
                    OrderFactory.UndeliveredOrder order = orderFactory.makeUndeliveredOrder(customerId, stockProvider);
                    db.populateUndeliveredOrder(order);
                }
            }
        }

        return orderFactory.getNextOrderId();
    }

    @Override
    public void close() {
        db.close();
    }
}
