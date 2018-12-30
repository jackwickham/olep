package net.jackw.olep.integration.matchers;

import net.jackw.olep.common.records.Credit;
import net.jackw.olep.message.transaction_result.NewOrderResult;
import net.jackw.olep.message.transaction_result.OrderLineResult;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.math.BigDecimal;
import java.util.List;

public class NewOrderResultMatcher extends TypeSafeDiagnosingMatcher<NewOrderResult> {
    private final int customerId;
    private final int districtId;
    private final int warehouseId;
    private final Matcher<Long> orderDate;
    private final Matcher<Integer> orderId;
    private final Matcher<String> customerLastName;
    private final Matcher<Credit> credit;
    private final Matcher<BigDecimal> discount;
    private final Matcher<BigDecimal> warehouseTax;
    private final Matcher<BigDecimal> districtTax;
    private final Matcher<Iterable<? extends OrderLineResult>> lines;

    public NewOrderResultMatcher(
        int customerId, int districtId, int warehouseId, Matcher<Long> orderDate, Matcher<Integer> orderId,
        Matcher<String> customerLastName, Matcher<Credit> credit, Matcher<BigDecimal> discount,
        Matcher<BigDecimal> warehouseTax, Matcher<BigDecimal> districtTax, Matcher<Iterable<? extends OrderLineResult>> lines
    ) {
        super(NewOrderResult.class);

        this.customerId = customerId;
        this.districtId = districtId;
        this.warehouseId = warehouseId;
        this.orderDate = orderDate;
        this.orderId = orderId;
        this.customerLastName = customerLastName;
        this.credit = credit;
        this.discount = discount;
        this.warehouseTax = warehouseTax;
        this.districtTax = districtTax;
        this.lines = lines;
    }

    @Override
    protected boolean matchesSafely(NewOrderResult result, Description mismatchDescription) {
        if (customerId != result.customerId) {
            mismatchDescription.appendText("customer ID ")
                .appendValue(result.customerId)
                .appendText(" did not equal ")
                .appendValue(customerId);
            return false;
        }
        if (districtId != result.districtId) {
            mismatchDescription.appendText("district ID ")
                .appendValue(result.districtId)
                .appendText(" did not equal ")
                .appendValue(districtId);
            return false;
        }
        if (warehouseId != result.warehouseId) {
            mismatchDescription.appendText("warehouse ID ")
                .appendValue(result.warehouseId)
                .appendText(" did not equal ")
                .appendValue(warehouseId);
            return false;
        }
        if (!orderDate.matches(result.orderDate)) {
            mismatchDescription.appendText("order date ");
            orderDate.describeMismatch(result.orderDate, mismatchDescription);
            return false;
        }
        if (!orderId.matches(result.orderId)) {
            mismatchDescription.appendText("order id ");
            orderId.describeMismatch(result.orderId, mismatchDescription);
            return false;
        }
        if (!customerLastName.matches(result.customerLastName)) {
            mismatchDescription.appendText("customer last name ");
            customerLastName.describeMismatch(result.customerLastName, mismatchDescription);
            return false;
        }
        if (!credit.matches(result.credit)) {
            mismatchDescription.appendText("credit ");
            credit.describeMismatch(result.credit, mismatchDescription);
            return false;
        }
        if (!discount.matches(result.discount)) {
            mismatchDescription.appendText("discount ");
            discount.describeMismatch(result.discount, mismatchDescription);
            return false;
        }
        if (!warehouseTax.matches(result.warehouseTax)) {
            mismatchDescription.appendText("warehouse tax ");
            warehouseTax.describeMismatch(result.warehouseTax, mismatchDescription);
            return false;
        }
        if (!districtTax.matches(result.districtTax)) {
            mismatchDescription.appendText("district tax ");
            districtTax.describeMismatch(result.districtTax, mismatchDescription);
            return false;
        }
        if (!lines.matches(result.lines)) {
            mismatchDescription.appendText("order lines ");
            lines.describeMismatch(result.lines, mismatchDescription);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendList("NewOrderResult(", ", ", ")", List.of(
            Matchers.equalTo(customerId), Matchers.equalTo(districtId), Matchers.equalTo(warehouseId), orderDate,
            orderId, customerLastName, credit, discount, warehouseTax, districtTax, lines
        ));
    }
}
