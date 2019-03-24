package net.jackw.olep.common;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SafeReadOnlyKeyValueStoreTest {
    private static final String STORE_NAME = "TEST_STORE";
    @Mock
    private KafkaStreams streams;

    @Mock
    private KeyValueStore<Object, Object> store;

    private Latch latch = new Latch(true);

    @Test
    public void testConstructorThrowsStoreUnavailableExceptionIfLatchNeverOpens() throws InterruptedException {
        latch.open();
        try {
            new FastSafeReadOnlyKeyValueStore();
            fail("Should have thrown an exception");
        } catch (SafeReadOnlyKeyValueStore.StoreUnavailableException e) {
            assertThat(e.getCause(), Matchers.instanceOf(TimeoutException.class));
            assertThat(e.getSuppressed(), Matchers.emptyArray());
        }
    }

    @Test
    public void testConstructorThrowsStoreUnavailableExceptionAfterMultipleKafkaExceptions() throws InterruptedException {
        when(streams.store(eq(STORE_NAME), any())).thenThrow(InvalidStateStoreException.class);
        try {
            new FastSafeReadOnlyKeyValueStore();
            fail("Should have thrown an exception");
        } catch (SafeReadOnlyKeyValueStore.StoreUnavailableException e) {
            assertThat(e.getCause(), Matchers.instanceOf(InvalidStateStoreException.class));
            assertThat(e.getSuppressed(), Matchers.arrayWithSize(Matchers.greaterThan(1)));
            assertThat(Arrays.asList(e.getSuppressed()), Matchers.everyItem(Matchers.instanceOf(InvalidStateStoreException.class)));
        }
    }

    @Test
    public void testConstructorSucceedsWhenOnlyOneError() throws InterruptedException, SafeReadOnlyKeyValueStore.StoreUnavailableException {
        when(streams.store(eq(STORE_NAME), any())).thenThrow(InvalidStateStoreException.class).thenReturn(store);
        new FastSafeReadOnlyKeyValueStore();
    }

    @Test
    public void testGetThrowsStoreUnavailableExceptionIfLatchDoesntClose() throws InterruptedException, SafeReadOnlyKeyValueStore.StoreUnavailableException {
        when(streams.store(eq(STORE_NAME), any())).thenReturn(store);
        FastSafeReadOnlyKeyValueStore s = new FastSafeReadOnlyKeyValueStore();
        latch.open();
        try {
            s.get(new Object());
            fail("Should have thrown an exception");
        } catch (SafeReadOnlyKeyValueStore.StoreUnavailableException e) {
            assertThat(e.getCause(), Matchers.instanceOf(TimeoutException.class));
            assertThat(e.getSuppressed(), Matchers.emptyArray());
            verifyNoMoreInteractions(store);
        }
    }

    @Test
    public void testGetThrowsStoreUnavailableExceptionAfterMultipleKafkaExceptions() throws InterruptedException, SafeReadOnlyKeyValueStore.StoreUnavailableException {
        Object key = new Object();
        when(streams.store(eq(STORE_NAME), any())).thenReturn(store);
        when(store.get(key)).thenThrow(InvalidStateStoreException.class);
        FastSafeReadOnlyKeyValueStore s = new FastSafeReadOnlyKeyValueStore();
        try {
            s.get(key);
            fail("Should have thrown an exception");
        } catch (SafeReadOnlyKeyValueStore.StoreUnavailableException e) {
            assertThat(e.getCause(), Matchers.instanceOf(InvalidStateStoreException.class));
            assertThat(e.getSuppressed(), Matchers.arrayWithSize(Matchers.greaterThan(1)));
            assertThat(Arrays.asList(e.getSuppressed()), Matchers.everyItem(Matchers.instanceOf(InvalidStateStoreException.class)));
        }
    }

    @Test
    public void testGetReturnsValueFromStoreWithoutSleeping() throws InterruptedException, SafeReadOnlyKeyValueStore.StoreUnavailableException {
        Object key = new Object();
        Object value = new Object();
        when(streams.store(eq(STORE_NAME), any())).thenReturn(store);
        when(store.get(key)).thenReturn(value);

        Thread.currentThread().interrupt();

        FastSafeReadOnlyKeyValueStore s = new FastSafeReadOnlyKeyValueStore();
        assertEquals(value, s.get(key));
        assertTrue(Thread.currentThread().isInterrupted());
    }

    private class FastSafeReadOnlyKeyValueStore extends SafeReadOnlyKeyValueStore<Object, Object> {
        public FastSafeReadOnlyKeyValueStore() throws InterruptedException, StoreUnavailableException {
            super(streams, STORE_NAME, latch);
        }

        @Override
        protected int getLatchTimeoutMs() {
            return 10;
        }
    }
}
