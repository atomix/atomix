package net.kuujo.copycat.log;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Chronicle log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ChronicleLogTest extends AbstractLogTest {

  /**
   * Tests configuring the buffered log.
   */
  public void testConfigurationDefaults() throws Throwable {
    Log log = new FileLog();
    assertEquals(log.getSegmentSize(), 1024 * 1024 * 1024);
    log.setSegmentSize(1024 * 1024);
    assertEquals(log.getSegmentSize(), 1024 * 1024);
    assertEquals(log.getSegmentInterval(), Long.MAX_VALUE);
    log.setSegmentInterval(60000);
    assertEquals(log.getSegmentInterval(), 60000);
  }

  /**
   * Tests configuring the buffered log via a configuration file.
   */
  public void testConfigurationFile() throws Throwable {
    Log log = new FileLog("log-test");
    assertEquals(log.getSegmentSize(), 1024 * 1024);
    assertEquals(log.getSegmentInterval(), 60000);
  }

  @AfterTest
  protected void cleanLogDir() throws IOException {
    Path directory = Paths.get("target/test-logs/");
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }


  @Override
  protected AbstractLogManager createLog() throws Throwable {
    String id = UUID.randomUUID().toString();
    return (AbstractLogManager) new ChronicleLog()
      .withSegmentSize(segmentSize)
      .withDirectory(new File(String.format("target/test-logs/%s", id)))
      .withIndexFileCapacity(1024 * 1024)
      .withIndexFileExcerpts(8 * 1024)
      .withDataBlockSize(8 * 1024)
      .withMessageCapacity(8192 / 2)
      .withMinimiseFootprint(true)
      .getLogManager(id);
  }

  @Override
  protected int entrySize() {
    return 17;
  }
}
