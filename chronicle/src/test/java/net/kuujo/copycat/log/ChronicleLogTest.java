package net.kuujo.copycat.log;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

/**
 * Chronicle log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ChronicleLogTest extends AbstractLogTest {
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
