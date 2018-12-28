package net.jackw.olep.integration;

import net.jackw.olep.common.records.Credit;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.Item;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.integration.matchers.DateMatcher;
import net.jackw.olep.integration.matchers.NewOrderResultMatcher;
import net.jackw.olep.integration.matchers.OrderLineResultMatcher;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import net.jackw.olep.utils.immutable_stores.PredictableCustomerFactory;
import net.jackw.olep.utils.immutable_stores.PredictableDistrictFactory;
import net.jackw.olep.utils.immutable_stores.PredictableItemFactory;
import net.jackw.olep.utils.immutable_stores.PredictableWarehouseFactory;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class WriteTransactionResponsesTest extends BaseIntegrationTest {
    @Before
    public void startListeners() {
        startVerifier();
        startWorker();
    }

    @Test
    public void newOrderTest() throws Throwable {
        PredictableItemFactory itemFactory = PredictableItemFactory.getInstance();
        PredictableWarehouseFactory warehouseFactory = PredictableWarehouseFactory.getInstance();
        PredictableDistrictFactory districtFactory = PredictableDistrictFactory.instanceFor(1);
        PredictableCustomerFactory customerFactory = PredictableCustomerFactory.instanceFor(2, 1, getCustomerNameRange());

        try (Database db = new Database(getEventBootsrapServers(), getViewBootstrapServers())) {
            TransactionResultHandler resultHandler = new TransactionResultHandler();

            List<NewOrderRequest.OrderLine> orderLines = new ArrayList<>(5);

            List<Matcher<? super OrderLineResult>> lineResultMatchers = new ArrayList<>(5);
            for (int i = 1; i <= 5; i++) {
                orderLines.add(new NewOrderRequest.OrderLine(i, 1, i));
                Item item = itemFactory.getItem(i);
                lineResultMatchers.add(new OrderLineResultMatcher(
                    1, i, equalTo(item.name), i, any(Integer.class), equalTo(item.price),
                    equalTo(item.price.multiply(new BigDecimal(i)))
                ));
            }

            CustomerShared customer = customerFactory.getCustomerShared(3);

            long startTime = System.currentTimeMillis();
            TransactionStatus<NewOrderResult> status = db.newOrder(3, 2, 1, orderLines);

            status.register(resultHandler.successListener(new NewOrderResultMatcher(
                3, 2, 1, new DateMatcher(startTime), is(1), equalTo(customer.lastName),
                equalTo(customer.credit), equalTo(customer.discount), equalTo(warehouseFactory.getWarehouseShared(1).tax),
                equalTo(districtFactory.getDistrictShared(2).tax), contains(lineResultMatchers)
            )));

            resultHandler.await();
        }
    }
}
