package net.kuujo.copycat;

import java.util.Set;

import org.junit.Test;

/**
 * Election tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ElectionTest extends CopyCatTest {

  @Test
  public void testLeaderElectEvent() throws Exception {
    new RunnableTest() {
      @Override
      public void run() throws Exception {
        Set<CopyCatContext> contexts = createCluster(3);
        contexts.iterator().next().election().addListener((event) -> testComplete());
        startCluster(contexts);
      }
    }.start();
  }

}
