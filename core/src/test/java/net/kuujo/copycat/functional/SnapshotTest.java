package net.kuujo.copycat.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.test.TestCluster;
import net.kuujo.copycat.test.TestLog;
import net.kuujo.copycat.test.TestNode;
import net.kuujo.copycat.test.TestStateMachine;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests snapshotting facilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class SnapshotTest {
  private Protocol protocol;
  private TestCluster cluster;

  @BeforeMethod
  protected void beforeMethod() {
    protocol = new LocalProtocol();
    cluster = new TestCluster();
  }

  @AfterMethod
  protected void afterMethod() {
    cluster.stop();
  }

  /**
   * Tests that the leader takes a snapshot of its state machine and compacts its log.
   */
  public void testLeaderTakesSnapshotAndCompactsLog() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node3 = new TestNode().withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("baz", "foo", "bar")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);

    cluster.addNodes(node1, node2, node3);
    cluster.start();

    node3.stateMachine().data("Hello world!");

    AtomicBoolean compacted = new AtomicBoolean();
    node3.log().on().compacted(() -> compacted.set(true));

    node1.instance().submit("command", "Hello world!").thenRun(() -> {
      node1.instance().submit("command", "Hello world!").thenRun(() -> {
        node1.instance().submit("command", "Hello world!").thenRun(() -> {
          node1.instance().submit("command", "Hello world!").thenRun(() -> {
            node1.instance().submit("command", "Hello world!").thenRun(() -> {
            });
          });
        });
      });
    });

    assertTrue(compacted.get());

    Entry firstEntry = node3.log().firstEntry();
    assertTrue(firstEntry instanceof SnapshotEntry);
    assertEquals("Hello world!", new String(((SnapshotEntry) firstEntry).data()));
  }

  /**
   * Tests that a follower takes a snapshot of its state machine and compacts its log.
   */
  public void testFollowerTakesSnapshotAndCompactsLog() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node3 = new TestNode().withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("baz", "foo", "bar")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNodes(node1, node2, node3);
    cluster.start();

    node1.stateMachine().data("Hello world!");

    AtomicBoolean compacted = new AtomicBoolean();
    node1.log().on().compacted(() -> compacted.set(true));

    node1.instance().submit("command", "Hello world!").thenRun(() -> {
      node1.instance().submit("command", "Hello world!").thenRun(() -> {
        node1.instance().submit("command", "Hello world!").thenRun(() -> {
          node1.instance().submit("command", "Hello world!").thenRun(() -> {
            node1.instance().submit("command", "Hello world!").thenRun(() -> {
            });
          });
        });
      });
    });

    assertTrue(compacted.get());

    Entry firstEntry = node1.log().firstEntry();
    assertTrue(firstEntry instanceof SnapshotEntry);
    assertEquals("Hello world!", new String(((SnapshotEntry) firstEntry).data()));
  }

  /**
   * Tests that the leader replicates its snapshot to far out of sync followers.
   */
  public void testLeaderReplicatesSnapshotToOutOfSyncFollowers() throws Exception {
    TestLog log1 = new TestLog();
    log1.appendEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")));
    for (long i = 0; i < 1000; i++) {
      log1.appendEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")));
    }
    SnapshotEntry snapshot1 = new SnapshotEntry(1, new Cluster("foo", "bar", "baz"),
      "Hello world!".getBytes());
    log1.compact(800, snapshot1);

    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withTerm(1)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(log1)
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(999)
      .withLastApplied(999);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withTerm(1)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(0)
      .withLastApplied(0);

    TestLog log3 = new TestLog();
    log1.appendEntry(new ConfigurationEntry(1, new Cluster("baz", "bar", "foo")));
    for (long i = 0; i < 1000; i++) {
      log3.appendEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")));
    }
    SnapshotEntry snapshot3 = new SnapshotEntry(1, new Cluster("baz", "bar", "foo"),
      "Hello world!".getBytes());
    log3.compact(800, snapshot3);

    TestNode node3 = new TestNode().withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withTerm(1)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(log3)
      .withState(CopycatState.LEADER)
      .withCommitIndex(999)
      .withLastApplied(999);
    cluster.addNodes(node1, node2, node3);
    cluster.start();

    assertEquals(800, node2.log().firstIndex());
    Entry firstEntry = node2.log().firstEntry();
    assertTrue(firstEntry instanceof SnapshotEntry);
    assertEquals("Hello world!", new String(((SnapshotEntry) firstEntry).data()));
  }
}
