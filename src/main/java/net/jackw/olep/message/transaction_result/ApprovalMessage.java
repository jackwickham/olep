package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class ApprovalMessage extends TransactionResultMessage {
    public final boolean approved;

    public ApprovalMessage(@JsonProperty("approved") boolean approved) {
        this.approved = approved;
    }
}
