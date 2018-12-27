package net.jackw.olep.integration.matchers;

import net.jackw.olep.message.transaction_result.OrderLineResult;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.math.BigDecimal;

public class OrderLineResultMatcher extends TypeSafeDiagnosingMatcher<OrderLineResult> {
    private final int supplyWarehouseId;
    private final int itemId;
    private final Matcher<String> itemName;
    private final int quantity;
    private final Matcher<Integer> stockQuantity;
    private final Matcher<BigDecimal> itemPrice;
    private final Matcher<BigDecimal> lineAmount;

    public OrderLineResultMatcher(
        int supplyWarehouseId, int itemId, Matcher<String> itemName, int quantity, Matcher<Integer> stockQuantity,
        Matcher<BigDecimal> itemPrice, Matcher<BigDecimal> lineAmount
    ) {
        super(OrderLineResult.class);
        this.supplyWarehouseId = supplyWarehouseId;
        this.itemId = itemId;
        this.itemName = itemName;
        this.quantity = quantity;
        this.stockQuantity = stockQuantity;
        this.itemPrice = itemPrice;
        this.lineAmount = lineAmount;
    }

    @Override
    protected boolean matchesSafely(OrderLineResult result, Description mismatchDescription) {
        if (supplyWarehouseId != result.supplyWarehouseId) {
            mismatchDescription.appendText("supply warehouse ID ")
                .appendValue(result.supplyWarehouseId)
                .appendText(" did not equal ")
                .appendValue(supplyWarehouseId);
            return false;
        }
        if (itemId != result.itemId) {
            mismatchDescription.appendText("item ID ")
                .appendValue(result.itemId)
                .appendText(" did not equal ")
                .appendValue(itemId);
            return false;
        }
        if (!itemName.matches(result.itemName)) {
            mismatchDescription.appendText("item name ");
            itemName.describeMismatch(result.itemName, mismatchDescription);
            return false;
        }
        if (quantity != result.quantity) {
            mismatchDescription.appendText("quantity ")
                .appendValue(result.quantity)
                .appendText(" did not equal ")
                .appendValue(quantity);
            return false;
        }
        if (!stockQuantity.matches(result.stockQuantity)) {
            mismatchDescription.appendText("stock quantity ");
            stockQuantity.describeMismatch(result.stockQuantity, mismatchDescription);
            return false;
        }
        if (!itemPrice.matches(result.itemPrice)) {
            mismatchDescription.appendText("item price ");
            itemPrice.describeMismatch(result.itemPrice, mismatchDescription);
            return false;
        }
        if (!lineAmount.matches(result.lineAmount)) {
            mismatchDescription.appendText("line amount ");
            lineAmount.describeMismatch(result.lineAmount, mismatchDescription);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendValueList("OrderLine(", ", ", ")", supplyWarehouseId, itemId);
    }
}
