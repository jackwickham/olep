package net.jackw.olep.integration;

import net.jackw.olep.common.records.Credit;
import net.jackw.olep.edge.Database;
import net.jackw.olep.edge.TransactionStatus;
import net.jackw.olep.integration.matchers.DateMatcher;
import net.jackw.olep.integration.matchers.NewOrderResultMatcher;
import net.jackw.olep.integration.matchers.OrderLineResultMatcher;
import net.jackw.olep.message.transaction_request.NewOrderRequest;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class WriteTransactionResponsesTest extends BaseIntegrationTest {
    @Before
    public void startListeners() {
        startVerifier();
        startWorker();
    }

    @Test
    public void test() throws Throwable {
        try (Database db = new Database(getEventBootsrapServers(), getViewBootstrapServers())) {
            TransactionResultHandler resultHandler = new TransactionResultHandler();

            List<NewOrderRequest.OrderLine> orderLines = List.of(
                new NewOrderRequest.OrderLine(1, 1, 1),
                new NewOrderRequest.OrderLine(2, 1, 2),
                new NewOrderRequest.OrderLine(3, 1, 3),
                new NewOrderRequest.OrderLine(4, 1, 4),
                new NewOrderRequest.OrderLine(5, 1, 5)
            );
            long startTime = System.currentTimeMillis();
            TransactionStatus<NewOrderResult> status = db.newOrder(3, 2, 1, orderLines);

            status.register(resultHandler.successListener(new NewOrderResultMatcher(
                3, 2, 1, new DateMatcher(startTime), is(1), any(String.class), any(Credit.class),
                any(BigDecimal.class), any(BigDecimal.class), any(BigDecimal.class), contains(
                    new OrderLineResultMatcher(1, 1, any(String.class), 1, any(Integer.class), any(BigDecimal.class), any(BigDecimal.class)),
                    new OrderLineResultMatcher(1, 2, any(String.class), 2, any(Integer.class), any(BigDecimal.class), any(BigDecimal.class)),
                    new OrderLineResultMatcher(1, 3, any(String.class), 3, any(Integer.class), any(BigDecimal.class), any(BigDecimal.class)),
                    new OrderLineResultMatcher(1, 4, any(String.class), 4, any(Integer.class), any(BigDecimal.class), any(BigDecimal.class)),
                    new OrderLineResultMatcher(1, 5, any(String.class), 5, any(Integer.class), any(BigDecimal.class), any(BigDecimal.class))
                )
            )));

            resultHandler.await();
        }
    }
}
