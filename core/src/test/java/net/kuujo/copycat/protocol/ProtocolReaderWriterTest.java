package net.kuujo.copycat.protocol;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;
import net.kuujo.copycat.log.Entry;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Protocol reader/writer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ProtocolReaderWriterTest {
  private ProtocolReader reader;
  private ProtocolWriter writer;

  @BeforeMethod
  public void beforeMethod() {
    reader = new ProtocolReader();
    writer = new ProtocolWriter();
  }

  /**
   * Tests writing/reading a ping request.
   */
  public void testWriteReadPingRequest() {
    PingRequest request = new PingRequest(UUID.randomUUID().toString(), 1, "foo", 4, 1, 3);
    PingRequest result = reader.readRequest(writer.writeRequest(request));
    assertEquals(result.id(), request.id());
    assertEquals(result.term(), request.term());
    assertEquals(result.leader(), request.leader());
    assertEquals(result.logIndex(), request.logIndex());
    assertEquals(result.logTerm(), request.logTerm());
    assertEquals(result.commitIndex(), request.commitIndex());
  }

  /**
   * Tests writing/reading a ping response.
   */
  public void testWriteReadPingResponse() {
    PingResponse response = new PingResponse(UUID.randomUUID().toString(), 1, true);
    PingResponse result = reader.readResponse(writer.writeResponse(response));
    assertEquals(result.id(), response.id());
    assertEquals(result.term(), response.term());
    assertEquals(result.succeeded(), response.succeeded());
  }

  /**
   * Tests writing/reading a sync request.
   */
  public void testWriteReadSyncRequest() {
    Cluster cluster = new Cluster("foo", "bar", "baz");
    SnapshotEntry snapshotEntry = new SnapshotEntry(1, cluster, new byte[50]);
    ConfigurationEntry configurationEntry = new ConfigurationEntry(1, cluster);
    OperationEntry operationEntry = new OperationEntry(1, "foo", "bar", "baz");
    List<Entry> entries = Arrays.asList(snapshotEntry, configurationEntry, operationEntry);
    SyncRequest request = new SyncRequest(UUID.randomUUID().toString(), 1, "foo", 4, 1, entries, 3);
    SyncRequest result = reader.readRequest(writer.writeRequest(request));
    assertEquals(result.id(), request.id());
    assertEquals(result.term(), request.term());
    assertEquals(result.leader(), request.leader());
    assertEquals(result.prevLogIndex(), request.prevLogIndex());
    assertEquals(result.prevLogTerm(), request.prevLogTerm());
    assertEquals(result.entries().size(), request.entries().size());
    assertEquals(result.entries().get(0), request.entries().get(0));
    assertEquals(result.entries().get(1), request.entries().get(1));
    assertEquals(result.entries().get(2), request.entries().get(2));
    assertEquals(result.commitIndex(), request.commitIndex());
  }

  /**
   * Tests writing/reading a sync response.
   */
  public void testWriteReadSyncResponse() {
    SyncResponse response = new SyncResponse(UUID.randomUUID().toString(), 1, true, 1);
    SyncResponse result = reader.readResponse(writer.writeResponse(response));
    assertEquals(result.id(), response.id());
    assertEquals(result.term(), response.term());
    assertEquals(result.succeeded(), response.succeeded());
    assertEquals(result.lastLogIndex(), response.lastLogIndex());
  }

  /**
   * Tests writing/reading a poll request.
   */
  public void testWriteReadPollRequest() {
    PollRequest request = new PollRequest(UUID.randomUUID().toString(), 1, "foo", 4, 1);
    PollRequest result = reader.readRequest(writer.writeRequest(request));
    assertEquals(result.id(), request.id());
    assertEquals(result.term(), request.term());
    assertEquals(result.candidate(), request.candidate());
    assertEquals(result.lastLogIndex(), request.lastLogIndex());
    assertEquals(result.lastLogTerm(), request.lastLogTerm());
  }

  /**
   * Tests writing/reading a poll response.
   */
  public void testWriteReadPollResponse() {
    PollResponse response = new PollResponse(UUID.randomUUID().toString(), 1, true);
    PollResponse result = reader.readResponse(writer.writeResponse(response));
    assertEquals(result.id(), response.id());
    assertEquals(result.term(), response.term());
    assertEquals(result.voteGranted(), response.voteGranted());
  }

  /**
   * Tests writing/reading a submit request.
   */
  public void testWriteReadSubmitRequest() {
    SubmitRequest request = new SubmitRequest(UUID.randomUUID().toString(), "foo", Arrays.asList(
      "bar", "baz"));
    SubmitRequest result = reader.readRequest(writer.writeRequest(request));
    assertEquals(result.id(), request.id());
    assertEquals(result.operation(), request.operation());
    assertEquals(result.args(), request.args());
  }

  /**
   * Tests writing/reading a submit response.
   */
  public void testWriteReadSubmitResponse() {
    SubmitResponse response = new SubmitResponse(UUID.randomUUID().toString(), "Hello world!");
    SubmitResponse result = reader.readResponse(writer.writeResponse(response));
    assertEquals(result.id(), response.id());
    assertEquals(result.result(), response.result());
  }

}
