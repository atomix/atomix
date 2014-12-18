package net.kuujo.copycat.log;

import java.util.UUID;

import net.openhft.chronicle.ChronicleConfig;

import org.testng.annotations.Test;

/**
 * Chronicle log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ChronicleLogTest extends AbstractLogTest {
  @Override
  protected AbstractLog createLog() throws Throwable {
    LogConfig config = new LogConfig().withSegmentSize(segmentSize);
    String id = UUID.randomUUID().toString();
    ChronicleConfig chronicleConfig = ChronicleConfig.DEFAULT.indexFileCapacity(1024 * 1024)
      .indexFileExcerpts(8 * 1024)
      .dataBlockSize(8 * 1024)
      .messageCapacity(8192 / 2);
    chronicleConfig.minimiseFootprint(true);
    return new ChronicleLog(id, config, chronicleConfig);
  }

  @Override
  protected int entrySize() {
    return 17;
  }
}
