package net.kuujo.copycat.io.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public abstract class AbstractLogTest {
  protected Log log;
  String logId;

  protected abstract Log createLog();

  @BeforeMethod
  void setLog() {
    logId = UUID.randomUUID().toString();
    log = createLog();
  }

  @AfterMethod
  protected void deleteLog() {
    log.delete();
  }

  protected Storage.Builder tempStorageBuilder() {
    return Storage.builder().withDirectory(new File(String.format("target/test-logs/%s", logId)));
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
}
