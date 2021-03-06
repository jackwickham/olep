package net.jackw.olep.acceptance.consistency;

import net.jackw.olep.acceptance.AppRunner;
import net.jackw.olep.acceptance.BaseAcceptanceTest;
import net.jackw.olep.acceptance.CurrentTestState;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Suite.SuiteClasses({
    ConsistencySuite.ConsistencyBeforeRunTest.class,
    ConsistencySuite.Run.class,
    ConsistencySuite.ConsistencyAfterTestRun.class
})
@RunWith(Suite.class)
public class ConsistencySuite extends BaseAcceptanceTest {
    public static class ConsistencyBeforeRunTest extends ConsistencyProperties { }

    public static class Run extends AppRunner {
        public Run() {
            super(120, CurrentTestState.getInstance().config);
        }

        @BeforeClass
        public static void checkPartOfSuite() {
            Assume.assumeTrue(CurrentTestState.hasInstance());
        }
    }

    public static class ConsistencyAfterTestRun extends ConsistencyProperties { }

    @BeforeClass
    public static void runStartDb() throws InterruptedException, ExecutionException, IOException {
        startDb();
    }

    @AfterClass
    public static void runShutdown() throws InterruptedException {
        shutdown();
    }
}
