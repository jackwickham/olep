package net.jackw.olep.integration.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class DateMatcher extends TypeSafeDiagnosingMatcher<Long> {
    private long startDate;

    public DateMatcher(long startDate) {
        this.startDate = startDate;
    }

    @Override
    protected boolean matchesSafely(Long item, Description mismatchDescription) {
        long now = System.currentTimeMillis();
        if (item >= startDate && item <= now) {
            return true;
        } else {
            mismatchDescription.appendValue(item)
                .appendText(" was not in the range [")
                .appendValue(startDate)
                .appendText(", ")
                .appendValue(now)
                .appendText("]");
            return false;
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("between ").appendValue(startDate).appendText(" and now");
    }
}
