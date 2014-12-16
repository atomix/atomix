package net.kuujo.copycat.log;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Chronicle log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ChronicleLogTest {
  Log log;

  @BeforeMethod
  protected void beforeMethod() {
    LogConfig config = new LogConfig().withSegmentSize(1000);
    String id = UUID.randomUUID().toString();
    log = new ChronicleLog(id, config);
    log.open();
  }

  @AfterMethod
  protected void afterMethod() {
    log.close();
    log.delete();
  }

  public void shouldOpenAndClose() {
    LogConfig config = new LogConfig().withSegmentSize(1000);
    String id = UUID.randomUUID().toString();
    Log otherLog = new ChronicleLog(id, config);

    try {
      assertTrue(otherLog.isClosed());
      assertFalse(otherLog.isOpen());
      otherLog.open();
      assertTrue(otherLog.isOpen());
      assertFalse(otherLog.isClosed());
      assertTrue(otherLog.isEmpty());
    } finally {
      otherLog.close();
      otherLog.delete();
    }
  }

  /**
   * Tests log segmenting.
   */
  public void testLogSegments() {
    assertTrue(log.isEmpty());
    appendEntries(log, 1000);
    assertTrue(log.segments().size() > 1);
    assertFalse(log.isEmpty());
    log.appendEntry(ByteBuffer.wrap("Hello world!".getBytes()));
    assertTrue(log.segments().size() > 1);
  }

  /**
   * Tests appending and getting entries.
   */
  public void testAppendGetEntries() {
    appendEntries(log, 5);
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));
    assertTrue(log.containsIndex(1));
    assertEquals(new String(log.getEntry(1).array()), "Hello world!");
    assertEquals(new String(log.getEntry(2).array()), "Hello world!");
    assertEquals(new String(log.getEntry(3).array()), "Hello world!");
    assertEquals(new String(log.getEntry(4).array()), "Hello world!");
    assertEquals(new String(log.getEntry(5).array()), "Hello world!");
    assertFalse(log.containsIndex(6));
    log.appendEntry(ByteBuffer.wrap("1".getBytes()));
    log.appendEntry(ByteBuffer.wrap("2".getBytes()));
    log.appendEntry(ByteBuffer.wrap("3".getBytes()));
    log.appendEntry(ByteBuffer.wrap("4".getBytes()));
    log.appendEntry(ByteBuffer.wrap("5".getBytes()));
    assertTrue(log.containsIndex(10));
    assertFalse(log.containsIndex(11));
    List<ByteBuffer> entries = log.getEntries(7, 9);
    assertEquals(entries.size(), 3);
    assertEquals(new String(entries.get(0).array()), "2");
    assertEquals(new String(entries.get(1).array()), "3");
    assertEquals(new String(entries.get(2).array()), "4");
  }

  /**
   * Tests replacing entries at the end of the log.
   */
  public void testRemoveReplaceEntries() {
    log.appendEntry(ByteBuffer.wrap("1".getBytes()));
    log.appendEntry(ByteBuffer.wrap("2".getBytes()));
    log.appendEntry(ByteBuffer.wrap("3".getBytes()));
    log.appendEntry(ByteBuffer.wrap("4".getBytes()));
    log.appendEntry(ByteBuffer.wrap("5".getBytes()));
    assertTrue(log.containsIndex(1));
    assertTrue(log.containsIndex(5));
    log.removeAfter(3);
    assertTrue(log.containsIndex(1));
    assertTrue(log.containsIndex(3));
    assertEquals(new String(log.getEntry(3).array()), "3");
    assertFalse(log.containsIndex(4));
    assertFalse(log.containsIndex(5));
    log.appendEntry(ByteBuffer.wrap("6".getBytes()));
    log.appendEntry(ByteBuffer.wrap("7".getBytes()));
    log.appendEntry(ByteBuffer.wrap("8".getBytes()));
    log.appendEntry(ByteBuffer.wrap("9".getBytes()));
    log.appendEntry(ByteBuffer.wrap("10".getBytes()));
    assertTrue(log.containsIndex(8));
    assertEquals(new String(log.getEntry(4).array()), "6");
    assertEquals(new String(log.getEntry(8).array()), "10");
  }

  public void shouldDeleteAfterIndex0() {
    log.appendEntry(ByteBuffer.wrap("1".getBytes()));
    log.removeAfter(0);
    assertTrue(log.isEmpty());
    assertEquals(log.size(), 0);
  }

  /**
   * Appends entries to the log.
   */
  private void appendEntries(Log log, int numEntries) {
    for (int i = 0; i < numEntries; i++) {
      log.appendEntry(ByteBuffer.wrap("Hello world!".getBytes()));
    }
  }
}
