package net.jackw.olep.integration.matchers;

import net.jackw.olep.message.transaction_result.DeliveryResult;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeliveryResultMatcher extends TypeSafeDiagnosingMatcher<DeliveryResult> {
    public final int warehouseId;
    public final int carrierId;
    public final Matcher<? super Set<Map.Entry<Integer, Integer>>> processedOrdersMatcher;

    public DeliveryResultMatcher(int warehouseId, int carrierId, Matcher<? super Set<Map.Entry<Integer, Integer>>> processedOrdersMatcher) {
        super(DeliveryResult.class);

        this.warehouseId = warehouseId;
        this.carrierId = carrierId;
        this.processedOrdersMatcher = processedOrdersMatcher;
    }

    @Override
    protected boolean matchesSafely(DeliveryResult result, Description mismatchDescription) {
        if (warehouseId != result.warehouseId) {
            mismatchDescription.appendText("warehouse ID ")
                .appendValue(result.warehouseId)
                .appendText(" did not equal ")
                .appendValue(warehouseId);
            return false;
        }
        if (carrierId != result.carrierId) {
            mismatchDescription.appendText("carrier ID ")
                .appendValue(result.carrierId)
                .appendText(" did not equal ")
                .appendValue(carrierId);
            return false;
        }
        if (!processedOrdersMatcher.matches(result.processedOrders.entrySet())) {
            mismatchDescription.appendText("processed orders ");
            processedOrdersMatcher.describeMismatch(result.processedOrders, mismatchDescription);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendList("DeliveryResult(", ", ", ")", List.of(
            Matchers.equalTo(warehouseId), Matchers.equalTo(carrierId), processedOrdersMatcher
        ));
    }
}
