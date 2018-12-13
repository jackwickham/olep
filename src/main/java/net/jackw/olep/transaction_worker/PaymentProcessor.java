package net.jackw.olep.transaction_worker;

import net.jackw.olep.common.LogConfig;
import net.jackw.olep.common.KafkaConfig;
import net.jackw.olep.common.SharedCustomerStore;
import net.jackw.olep.common.SharedKeyValueStore;
import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerMutable;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictShared;
import net.jackw.olep.common.records.DistrictSpecificKey;
import net.jackw.olep.common.records.WarehouseShared;
import net.jackw.olep.common.records.WarehouseSpecificKey;
import net.jackw.olep.message.transaction_request.PaymentRequest;
import net.jackw.olep.message.transaction_result.PaymentResult;
import net.jackw.olep.message.transaction_result.TransactionResultMessage;
import net.jackw.olep.utils.RandomDataGenerator;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

public class PaymentProcessor extends BaseTransactionProcessor implements Processor<Long, PaymentRequest> {
    private ProcessorContext context;
    private LocalStore<DistrictSpecificKey, CustomerMutable> customerMutableStore;

    private final SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore;
    private final SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore;
    private final SharedCustomerStore customerImmutableStore;

    private final RandomDataGenerator rand = new RandomDataGenerator();

    public PaymentProcessor(
        SharedKeyValueStore<Integer, WarehouseShared> warehouseImmutableStore,
        SharedKeyValueStore<WarehouseSpecificKey, DistrictShared> districtImmutableStore,
        SharedCustomerStore customerImmutableStore,
        int acceptedTransactionsPartitions
    ) {
        super(acceptedTransactionsPartitions);
        this.warehouseImmutableStore = warehouseImmutableStore;
        this.districtImmutableStore = districtImmutableStore;
        this.customerImmutableStore = customerImmutableStore;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;

        this.customerMutableStore = new LocalStore<DistrictSpecificKey, CustomerMutable>(
            (KeyValueStore) context.getStateStore(KafkaConfig.CUSTOMER_MUTABLE_STORE),
            () -> new CustomerMutable(new BigDecimal(-10), rand.aString(12, 24))
        );
    }

    @Override
    public void process(Long key, PaymentRequest value) {
        try {
            log.debug(LogConfig.TRANSACTION_ID_MARKER, "Processing payment transaction with id {}", key);
            final PaymentResult.PartialResult results = new PaymentResult.PartialResult();

            CustomerShared customer;
            if (value.customerId != null) {
                customer = customerImmutableStore.getBlocking(
                    new DistrictSpecificKey(value.customerId, value.districtId, value.warehouseId)
                );
            } else {
                customer = customerImmutableStore.getBlocking(
                    new CustomerNameKey(value.warehouseId, value.districtId, value.customerSurname)
                );
            }

            CustomerMutable oldCustomerMutable = customerMutableStore.get(customer.getKey());
            String newData;
            if (customer.credit == Credit.BC) {
                StringBuilder builder = new StringBuilder(Math.min(oldCustomerMutable.data.length() + 20, 500));
                builder.append(customer.id)
                    .append(customer.districtId)
                    .append(customer.warehouseId)
                    .append(value.districtId)
                    .append(value.warehouseId)
                    .append(value.amount);
                builder.append(oldCustomerMutable.data, 0, Math.min(oldCustomerMutable.data.length(), 500 - builder.length()));
                newData = builder.toString();
            } else {
                newData = oldCustomerMutable.data;
            }
            CustomerMutable customerMutable = new CustomerMutable(
                oldCustomerMutable.balance.subtract(value.amount), newData
            );
            customerMutableStore.put(customer.getKey(), customerMutable);

            WarehouseShared warehouse = warehouseImmutableStore.getBlocking(value.warehouseId);
            DistrictShared district = districtImmutableStore.getBlocking(
                new WarehouseSpecificKey(value.warehouseId, value.districtId)
            );

            results.warehouseAddress = warehouse.address;
            results.districtAddress = district.address;
            results.customerId = customer.id;
            results.customerAddress = customer.address;
            results.customerPhone = customer.phone;
            results.customerSince = customer.since;
            results.customerCredit = customer.credit;
            results.customerCreditLimit = customer.creditLimit;
            results.customerDiscount = customer.discount;
            results.customerBalance = customerMutable.balance;

            if (customer.credit == Credit.BC) {
                results.customerData = customerMutable.data.substring(0, Math.min(customerMutable.data.length(), 200));
            }

            TransactionResultMessage resultMessage = new TransactionResultMessage(key, results);
            context.forward(key, resultMessage, To.child("transaction-results"));

            // The PaymentRequest is also the modification record, so just send that to the modification log
            context.forward(key, value, To.child("modification-log"));

            // TPC-C says we should create a history record (and cast it into the abyss)
            // We could do that, but for now it can be derived by a consumer if they so desire
            // The entire system is made of history records
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    @Override
    public void close() {

    }

    private static Logger log = LogManager.getLogger();
}
