package net.jackw.olep.message.transaction_result;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class ApprovalMessage extends TransactionResultMessage {
    public final boolean approved;

    public ApprovalMessage(@JsonProperty("approved") boolean approved) {
        this.approved = approved;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(approved);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ApprovalMessage) {
            return approved == ((ApprovalMessage) obj).approved;
        } else {
            return false;
        }
    }
}
