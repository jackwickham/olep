package net.jackw.olep.acceptance.consistency;

import net.jackw.olep.acceptance.BaseAcceptanceTest;
import net.jackw.olep.acceptance.CurrentTestState;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@Suite.SuiteClasses({ConsistencySuite.ConsistencyBeforeRunTest.class})
@RunWith(Suite.class)
public class ConsistencySuite extends BaseAcceptanceTest {
    public static class ConsistencyBeforeRunTest extends ConsistencyProperties {
        @BeforeClass
        public static void checkPartOfSuite() {
            Assume.assumeTrue(CurrentTestState.hasInstance());
        }
    }
}
