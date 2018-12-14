package net.jackw.olep;

import org.apache.kafka.streams.processor.MockProcessorContext;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class ForwardedMessageMatcher<K, V> extends TypeSafeMatcher<MockProcessorContext.CapturedForward> {
    private final String topic;
    private final K expectedKey;
    private final V expectedValue;

    public ForwardedMessageMatcher(String topic, K expectedKey, V expectedValue) {
        super(MockProcessorContext.CapturedForward.class);

        this.topic = topic;
        this.expectedKey = expectedKey;
        this.expectedValue = expectedValue;
    }

    @Override
    protected boolean matchesSafely(MockProcessorContext.CapturedForward item) {
        return topic.equals(item.childName()) &&
            expectedKey.equals(item.keyValue().key) &&
            expectedValue.equals(item.keyValue().value);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("matchesForwarded(")
            .appendValue(topic)
            .appendText(", (")
            .appendValue(expectedKey)
            .appendText(", ")
            .appendValue(expectedValue)
            .appendText("))");
    }
}
