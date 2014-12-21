package net.kuujo.copycat.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import net.openhft.chronicle.ChronicleConfig;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

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
  protected AbstractLog createLog() throws Throwable {
    String id = UUID.randomUUID().toString();
    LogConfig config = new LogConfig().withSegmentSize(segmentSize).withDirectory(
      new File(String.format("target/test-logs/%s", id)));
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
