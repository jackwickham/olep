package net.jackw.olep.common;

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
     * @return The customer associated with that key
     */
    @Nonnull
    default CustomerShared getBlocking(CustomerNameKey key) throws InterruptedException {
        int attempts = 0;
        CustomerShared result;
        while ((result = get(key)) == null) {
            if (attempts >= BACKOFF_ATTEMPTS) {
                throw new StoreKeyMissingException(key);
            }
            Thread.sleep(100 + 100 * attempts++);
        }
        return result;
    }
}
