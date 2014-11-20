package net.kuujo.copycat.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MemberConfig;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;
import net.kuujo.copycat.log.Entry;

import org.testng.Assert;
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
    Assert.assertEquals(result.id(), request.id());
    Assert.assertEquals(result.term(), request.term());
    Assert.assertEquals(result.leader(), request.leader());
    Assert.assertEquals(result.logIndex(), request.logIndex());
    Assert.assertEquals(result.logTerm(), request.logTerm());
    Assert.assertEquals(result.commitIndex(), request.commitIndex());
  }

  /**
   * Tests writing/reading a ping response.
   */
  public void testWriteReadPingResponse() {
    PingResponse response = new PingResponse(UUID.randomUUID().toString(), 1, true);
    PingResponse result = reader.readResponse(writer.writeResponse(response));
    Assert.assertEquals(result.id(), response.id());
    Assert.assertEquals(result.term(), response.term());
    Assert.assertEquals(result.succeeded(), response.succeeded());
  }

  /**
   * Tests writing/reading a sync request.
   */
  public void testWriteReadSyncRequest() {
    ClusterConfig<Member> clusterConfig = new ClusterConfig<>()
      .withLocalMember(new Member(new MemberConfig("foo")))
      .withRemoteMembers(new Member(new MemberConfig("bar")), new Member(new MemberConfig("baz")));
    SnapshotEntry snapshotEntry = new SnapshotEntry(1, clusterConfig, new byte[50]);
    ConfigurationEntry configurationEntry = new ConfigurationEntry(1, clusterConfig);
    OperationEntry operationEntry = new OperationEntry(1, "foo", Arrays.asList("bar", "baz"));
    List<Entry> entries = Arrays.asList(snapshotEntry, configurationEntry, operationEntry);
    SyncRequest request = new SyncRequest(UUID.randomUUID().toString(), 1, "foo", 4, 1, entries, 3);
    SyncRequest result = reader.readRequest(writer.writeRequest(request));
    Assert.assertEquals(result.id(), request.id());
    Assert.assertEquals(result.term(), request.term());
    Assert.assertEquals(result.leader(), request.leader());
    Assert.assertEquals(result.prevLogIndex(), request.prevLogIndex());
    Assert.assertEquals(result.prevLogTerm(), request.prevLogTerm());
    Assert.assertEquals(result.entries().size(), request.entries().size());
    Assert.assertEquals(result.entries().get(0), request.entries().get(0));
    Assert.assertEquals(result.entries().get(1), request.entries().get(1));
    Assert.assertEquals(result.entries().get(2), request.entries().get(2));
    Assert.assertEquals(result.commitIndex(), request.commitIndex());
  }

  /**
   * Tests writing/reading a sync response.
   */
  public void testWriteReadSyncResponse() {
    SyncResponse response = new SyncResponse(UUID.randomUUID().toString(), 1, true, 1);
    SyncResponse result = reader.readResponse(writer.writeResponse(response));
    Assert.assertEquals(result.id(), response.id());
    Assert.assertEquals(result.term(), response.term());
    Assert.assertEquals(result.succeeded(), response.succeeded());
    Assert.assertEquals(result.lastLogIndex(), response.lastLogIndex());
  }

  /**
   * Tests writing/reading a poll request.
   */
  public void testWriteReadPollRequest() {
    PollRequest request = new PollRequest(UUID.randomUUID().toString(), 1, "foo", 4, 1);
    PollRequest result = reader.readRequest(writer.writeRequest(request));
    Assert.assertEquals(result.id(), request.id());
    Assert.assertEquals(result.term(), request.term());
    Assert.assertEquals(result.candidate(), request.candidate());
    Assert.assertEquals(result.lastLogIndex(), request.lastLogIndex());
    Assert.assertEquals(result.lastLogTerm(), request.lastLogTerm());
  }

  /**
   * Tests writing/reading a poll response.
   */
  public void testWriteReadPollResponse() {
    PollResponse response = new PollResponse(UUID.randomUUID().toString(), 1, true);
    PollResponse result = reader.readResponse(writer.writeResponse(response));
    Assert.assertEquals(result.id(), response.id());
    Assert.assertEquals(result.term(), response.term());
    Assert.assertEquals(result.voteGranted(), response.voteGranted());
  }

  /**
   * Tests writing/reading a submit request.
   */
  public void testWriteReadSubmitRequest() {
    SubmitRequest request = new SubmitRequest(UUID.randomUUID().toString(), "foo", Arrays.asList("bar", "baz"));
    SubmitRequest result = reader.readRequest(writer.writeRequest(request));
    Assert.assertEquals(result.id(), request.id());
    Assert.assertEquals(result.operation(), request.operation());
    Assert.assertEquals(result.args(), request.args());
  }

  /**
   * Tests writing/reading a submit response.
   */
  public void testWriteReadSubmitResponse() {
    SubmitResponse response = new SubmitResponse(UUID.randomUUID().toString(), "Hello world!");
    SubmitResponse result = reader.readResponse(writer.writeResponse(response));
    Assert.assertEquals(result.id(), response.id());
    Assert.assertEquals(result.result(), response.result());
  }

}
