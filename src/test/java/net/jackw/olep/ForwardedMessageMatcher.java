package net.jackw.olep;

import org.apache.kafka.streams.processor.MockProcessorContext;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

public class ForwardedMessageMatcher<K, V> extends TypeSafeMatcher<MockProcessorContext.CapturedForward> {
    private final String topic;
    private final Matcher<K> keyMatcher;
    private final Matcher<V> valueMatcher;

    public ForwardedMessageMatcher(String topic, K expectedKey, V expectedValue) {
        this(topic, Matchers.equalTo(expectedKey), Matchers.equalTo(expectedValue));
    }

    public ForwardedMessageMatcher(String topic, K expectedKey, Matcher<V> valueMatcher) {
        this(topic, Matchers.equalTo(expectedKey), valueMatcher);
    }

    public ForwardedMessageMatcher(String topic, Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
        super(MockProcessorContext.CapturedForward.class);

        this.topic = topic;
        this.keyMatcher = keyMatcher;
        this.valueMatcher = valueMatcher;
    }

    @Override
    protected boolean matchesSafely(MockProcessorContext.CapturedForward item) {
        return topic.equals(item.childName()) &&
            keyMatcher.matches(item.keyValue().key) &&
            valueMatcher.matches(item.keyValue().value);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matchesForwarded(")
            .appendValue(topic)
            .appendText(", (")
            .appendValue(keyMatcher)
            .appendText(", ")
            .appendValue(valueMatcher)
            .appendText("))");
    }
}
