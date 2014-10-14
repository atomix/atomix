package net.kuujo.copycat.functional;

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;
import net.kuujo.copycat.test.TestCluster;
import net.kuujo.copycat.test.TestLog;
import net.kuujo.copycat.test.TestNode;
import net.kuujo.copycat.test.TestStateMachine;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests snapshotting facilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class SnapshotTest {

  /**
   * Tests that the leader takes a snapshot of its state machine and compacts its log.
   */
  public void testLeaderTakesSnapshotAndCompactsLog() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode()
      .withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("foo"))
          .withRemoteMembers(new Member("bar"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node1);

    TestNode node2 = new TestNode()
      .withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node2);

    TestNode node3 = new TestNode()
      .withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("baz"))
          .withRemoteMembers(new Member("foo"), new Member("bar"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node3);

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

    Assert.assertTrue(compacted.get());

    Entry firstEntry = node3.log().firstEntry();
    Assert.assertTrue(firstEntry instanceof SnapshotEntry);
    Assert.assertEquals("Hello world!", new String(((SnapshotEntry) firstEntry).data()));

    cluster.stop();
  }

  /**
   * Tests that a follower takes a snapshot of its state machine and compacts its log.
   */
  public void testFollowerTakesSnapshotAndCompactsLog() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode()
      .withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("foo"))
          .withRemoteMembers(new Member("bar"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node1);

    TestNode node2 = new TestNode()
      .withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node2);

    TestNode node3 = new TestNode()
      .withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withConfig(new CopycatConfig().withMaxLogSize(512))
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("baz"))
          .withRemoteMembers(new Member("foo"), new Member("bar"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node3);

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

    Assert.assertTrue(compacted.get());

    Entry firstEntry = node1.log().firstEntry();
    Assert.assertTrue(firstEntry instanceof SnapshotEntry);
    Assert.assertEquals("Hello world!", new String(((SnapshotEntry) firstEntry).data()));

    cluster.stop();
  }

  /**
   * Tests that the leader replicates its snapshot to far out of sync followers.
   */
  public void testLeaderReplicatesSnapshotToOutOfSyncFollowers() throws Exception {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();

    TestLog log1 = new TestLog();
    log1.appendEntry(new ConfigurationEntry(1, new ClusterConfig()
      .withLocalMember(new Member("foo"))
      .withRemoteMembers(new Member("bar"), new Member("baz"))));
    for (long i = 0; i < 1000; i++) {
      log1.appendEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")));
    }
    SnapshotEntry snapshot1 = new SnapshotEntry(1, new ClusterConfig()
      .withLocalMember(new Member("foo"))
      .withRemoteMembers(new Member("bar"), new Member("baz")),
      "Hello world!".getBytes());
    log1.compact(800, snapshot1);

    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode()
      .withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withTerm(1)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(log1)
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(999)
      .withLastApplied(999);
    cluster.addNode(node1);

    TestNode node2 = new TestNode()
      .withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withTerm(1)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(0)
      .withLastApplied(0);
    cluster.addNode(node2);

    TestLog log3 = new TestLog();
    log1.appendEntry(new ConfigurationEntry(1, new ClusterConfig()
      .withLocalMember(new Member("baz"))
      .withRemoteMembers(new Member("bar"), new Member("foo"))));
    for (long i = 0; i < 1000; i++) {
      log3.appendEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")));
    }
    SnapshotEntry snapshot3 = new SnapshotEntry(1, new ClusterConfig()
      .withLocalMember(new Member("baz"))
      .withRemoteMembers(new Member("bar"), new Member("foo")),
      "Hello world!".getBytes());
    log3.compact(800, snapshot3);

    TestNode node3 = new TestNode()
      .withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withTerm(1)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(log3)
      .withState(CopycatState.LEADER)
      .withCommitIndex(999)
      .withLastApplied(999);
    cluster.addNode(node3);

    cluster.start();

    Assert.assertEquals(800, node2.log().firstIndex());
    Entry firstEntry = node2.log().firstEntry();
    Assert.assertTrue(firstEntry instanceof SnapshotEntry);
    Assert.assertEquals("Hello world!", new String(((SnapshotEntry) firstEntry).data()));

    cluster.stop();
  }

}
