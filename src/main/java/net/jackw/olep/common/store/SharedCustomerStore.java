package net.jackw.olep.common.store;

import net.jackw.olep.common.StoreKeyMissingException;
import net.jackw.olep.common.records.CustomerNameKey;
import net.jackw.olep.common.records.CustomerShared;
import net.jackw.olep.common.records.DistrictSpecificKey;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Public interface for customer stores, which provide additional get methods
 */
public interface SharedCustomerStore extends SharedKeyValueStore<DistrictSpecificKey, CustomerShared> {
    /**
     * Get the customer associated with a particular name
     */
    @Nullable
    CustomerShared get(CustomerNameKey key);

    /**
     * Get the customer with a particular name, blocking until the value becomes available. Blocking time is limited,
     * and if no customer can be found after the timeout a {@link StoreKeyMissingException} will be thrown.
     *
     * @param key The customer's name, district and warehouse
     * @param maxAttempts The number of times to retry, or 0 for unlimited
     * @return The customer associated with that key
     */
    @Nonnull
    default CustomerShared getBlocking(CustomerNameKey key, int maxAttempts) throws InterruptedException {
        int attempts = 0;
        CustomerShared result;
        while ((result = get(key)) == null) {
            if (maxAttempts > 0 && attempts >= maxAttempts) {
                throw new StoreKeyMissingException(key);
            }
            Thread.sleep(50);
            attempts++;
        }
        return result;
    }

    /**
     * Get the customer with a particular name, blocking until the value becomes available. Blocking time is limited,
     * and if no customer can be found after the timeout a {@link StoreKeyMissingException} will be thrown.
     *
     * 20 attempts will be made (for a maximum blocking time of approximately 1 second) before the exception is thrown.
     *
     * @param key The customer's name, district and warehouse
     * @return The customer associated with that key
     */
    @Nonnull
    default CustomerShared getBlocking(CustomerNameKey key) throws InterruptedException {
        return getBlocking(key, DEFAULT_BACKOFF_ATTEMPTS);
    }
}
