package net.jackw.olep.integration.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.List;
import java.util.Map;

public class MapEntryMatcher<K, V> extends TypeSafeDiagnosingMatcher<Map.Entry<K, V>> {
    public final K key;
    public final V value;

    public MapEntryMatcher(K key, V value) {
        super();

        this.key = key;
        this.value = value;
    }

    @Override
    protected boolean matchesSafely(Map.Entry<K, V> entry, Description mismatchDescription) {
        if (!key.equals(entry.getKey())) {
            mismatchDescription.appendText("key ")
                .appendValue(entry.getKey())
                .appendText(" did not equal ")
                .appendValue(key);
            return false;
        }
        if (!value.equals(entry.getValue())) {
            mismatchDescription.appendText("value ")
                .appendValue(entry.getValue())
                .appendText(" did not equal ")
                .appendValue(value);
            return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendValueList("(", ", ", ")", List.of(key, value));
    }
}
