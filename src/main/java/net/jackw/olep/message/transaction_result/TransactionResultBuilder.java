package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Builder class for T. Once canBuild returns true, build will return a T.
 *
 * The actual mutable fields will be in the PartialTransactionResult that instances of this interface will usually
 * extend - implementors of this interface will provide the construction methods and any static data (which doesn't
 * need to be returned from the DB).
 *
 * This class will be used by the edge to construct the result to send to the application.
 *
 * @param <T> The type that is built by this class
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public interface TransactionResultBuilder<T extends TransactionResult> extends PartialTransactionResult {
    boolean canBuild();
    T build();
}
