package net.jackw.olep.message.transaction_result;

import com.google.errorprone.annotations.Immutable;

@Immutable
public class TestResult extends TransactionResult {
    public static class Builder extends TransactionResultBuilder<TestResult> {
        @Override
        public boolean canBuild() {
            return rnd != null && hello != null;
        }

        @Override
        public TestResult build() {
            return new TestResult(rnd, hello);
        }

        public Integer rnd;
        public String hello;
    }

    public final int rnd;
    public final String hello;

    private TestResult(int rnd, String hello) {
        this.rnd = rnd;
        this.hello = hello;
    }
}
