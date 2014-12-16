package net.kuujo.copycat.log;

import java.util.UUID;

import org.testng.annotations.Test;

/**
 * Chronicle log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ChronicleLogTest extends AbstractLogTest {
  @Override
  protected Log createLog() throws Throwable {
    LogConfig config = new LogConfig().withSegmentSize(1000);
    String id = UUID.randomUUID().toString();
    return new ChronicleLog(id, config);
  }
}
