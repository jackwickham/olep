package net.jackw.olep.edge.transaction_result;

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

    public int rnd;
    public String hello;

    private TestResult(int rnd, String hello) {
        this.rnd = rnd;
        this.hello = hello;
    }
}
