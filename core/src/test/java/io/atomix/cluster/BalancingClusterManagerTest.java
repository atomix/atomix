package io.atomix.cluster;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test
public class BalancingClusterManagerTest extends ConcurrentTestCase {

  private int port;
  private LocalServerRegistry registry;
  private List<Address> members;
  private List<AtomixReplica> replicas;

  @BeforeMethod
  private void beforeMethod() {
    init();
  }

  @AfterMethod
  private void afterMethod() throws Throwable {
    cleanup();
  }

  /**
   * Tests promoting a new passive member to active when quorum hint is {@link Quorum#ALL}
   */
  public void testPromotingOnPassiveJoin() throws Throwable {
    testBalancingOnMemberJoin(AtomixReplica.Type.PASSIVE, Member.Type.ACTIVE);
  }

  /**
   * Tests promoting a new passive member to active when quorum hint is {@link Quorum#ALL}
   */
  public void testPromotingOnReserveJoin() throws Throwable {
    testBalancingOnMemberJoin(AtomixReplica.Type.RESERVE, Member.Type.ACTIVE);
  }

  /**
   * Tests promoting a new passive member to active when member leaves and quorum hint is not {@link Quorum#ALL}
   */
  public void testPromotingPassiveMemberOnMemberLeave() throws Throwable {
    testBalancingOnMemberLeave(AtomixReplica.Type.PASSIVE, Member.Type.ACTIVE);
  }

  /**
   * Tests promoting a new passive member to active when member leaves and quorum hint is not {@link Quorum#ALL}
   */
  public void testPromotingReserveMemberOnMemberLeave() throws Throwable {
    testBalancingOnMemberLeave(AtomixReplica.Type.RESERVE, Member.Type.ACTIVE);
  }

  /**
   * Tests demoting an active member to passive when number of active members are more than required and quorum hint is not {@link Quorum#ALL}
   */
  public void testDemotingActiveMemberToPassive() throws Throwable {
    ClusterManager clusterManager = BalancingClusterManager.builder().withQuorumHint(3).withBackupCount(1).build();
    List<AtomixReplica> active = createReplicas(3, clusterManager);
    Cluster cluster = active.get(0).server().server().cluster();
    cluster.members().stream().forEach(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.PASSIVE) {
          resume();
        }
      });
    });
    cluster.onJoin(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.PASSIVE) {
          resume();
        }
      });
    });

    // adding an ACTIVE member results in demotion of any ACTIVE member to PASSIVE
    AtomixReplica joiner = createReplica(nextAddress(), AtomixReplica.Type.ACTIVE, clusterManager);
    joiner.join(this.members).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests demoting an active member to passive when active members are more than required and quorum hint is not {@link Quorum#ALL}
   */
  public void testDemotingActiveMemberToReserve() throws Throwable {
    ClusterManager clusterManager = BalancingClusterManager.builder().withQuorumHint(3).withBackupCount(1).build();
    createReplicas(3, clusterManager);

    // add 2 passive member to the cluster to fulfill the required number of passive members
    List<AtomixReplica> passives = join(clusterManager, AtomixReplica.Type.PASSIVE, AtomixReplica.Type.PASSIVE);
    Cluster cluster = passives.get(0).server().server().cluster();
    cluster.members().stream().filter(m -> m.type() == Member.Type.ACTIVE).forEach(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.RESERVE) {
          resume();
        }
      });
    });
    cluster.onJoin(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.RESERVE) {
          resume();
        }
      });
    });

    // now adding a PASSIVE member results in demotion of any ACTIVE member to RESERVE
    AtomixReplica joiner = createReplica(nextAddress(), AtomixReplica.Type.ACTIVE, clusterManager);
    joiner.join(this.members).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests promoting an reserve member to passive when number of passive servers are less than required and quorum hint is not {@link Quorum#ALL}
   */
  public void testPromotingReserveMemberToPassive() throws Throwable {
    ClusterManager clusterManager = BalancingClusterManager.builder().withQuorumHint(3).withBackupCount(1).build();
    List<AtomixReplica> actives = createReplicas(3, clusterManager);
    join(clusterManager, AtomixReplica.Type.PASSIVE);
    Cluster cluster = actives.get(0).server().server().cluster();
    cluster.onJoin(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.PASSIVE) {
          resume();
        }
      });
    });

    // adding a RESERVE member will result in promotion of this member to PASSIVE
    AtomixReplica joiner = createReplica(nextAddress(), AtomixReplica.Type.RESERVE, clusterManager);
    joiner.join(this.members).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests promoting an reserve member to passive when number of passive servers are more than required and quorum hint is not {@link Quorum#ALL}
   */
  public void testDemotingPassiveMemberToReserve() throws Throwable {
    ClusterManager clusterManager = BalancingClusterManager.builder().withQuorumHint(3).withBackupCount(1).build();
    createReplicas(3, clusterManager);

    // add required number of passive members i.e. 2
    List<AtomixReplica> passives = join(clusterManager, AtomixReplica.Type.PASSIVE, AtomixReplica.Type.PASSIVE);
    Cluster cluster = passives.get(0).server().server().cluster();
    cluster.members().stream().filter(m -> m.type() == Member.Type.PASSIVE).forEach(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.RESERVE) {
          resume();
        }
      });
    });
    cluster.onJoin(m -> {
      m.onTypeChange(type -> {
        if (type == Member.Type.RESERVE) {
          resume();
        }
      });
    });

    // now add the extra passive member which results in demotion of any PASSIVE member to RESERVER
    AtomixReplica joiner = createReplica(nextAddress(), AtomixReplica.Type.PASSIVE, clusterManager);
    joiner.join(this.members).thenRun(this::resume);
    await(10000, 2);
  }

  private void testBalancingOnMemberLeave(AtomixReplica.Type newMemberType, Member.Type expected) throws Throwable {
    ClusterManager clusterManager = BalancingClusterManager.builder().withQuorumHint(3).withBackupCount(1).build();
    List<AtomixReplica> actives = createReplicas(3, clusterManager);
    actives.get(0).server().server().cluster().onJoin(m -> {
      m.onTypeChange(type -> {
        threadAssertEquals(expected, type);
        resume();
      });
    });

    AtomixReplica joiner = createReplica(nextAddress(), newMemberType, clusterManager);
    joiner.join(this.members).thenRun(this::resume);
    await(3000, 1);

    // remove an ACTIVE member
    AtomixReplica leaver = actives.get(0);
    leaver.leave().thenRun(this::resume);
    await(10000, 2);
  }

  private void testBalancingOnMemberJoin(AtomixReplica.Type newMemberType, Member.Type expectedType) throws Throwable {
    List<AtomixReplica> actives = createReplicas(3, BalancingClusterManager.builder().build());
    AtomixReplica replica = actives.get(0);
    replica.server().server().cluster().onJoin(m -> {
      m.onTypeChange(type -> {
        threadAssertEquals(expectedType, type);
        resume();
      });
    });

    AtomixReplica joiner = createReplica(nextAddress(), newMemberType);
    joiner.join(members).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Returns the next server address.
   */
  private Address nextAddress() {
    return new Address("localhost", port++);
  }

  /**
   * Creates a replicas with the given address and type
   */
  private AtomixReplica createReplica(Address address, AtomixReplica.Type type) {
    return createReplica(address, type, BalancingClusterManager.builder().build());
  }

  /**
   * Creates replica with given address, type and cluster manager
   */
  private AtomixReplica createReplica(Address address, AtomixReplica.Type type, ClusterManager clusterManager) {
    AtomixReplica replica = AtomixReplica.builder(address)
      .withClusterManager(clusterManager)
      .withTransport(new LocalTransport(registry))
      .withStorage(new Storage(StorageLevel.MEMORY))
      .withType(type)
      .build();
    replica.serializer().disableWhitelist();
    replicas.add(replica);
    return replica;
  }

  /**
   * Creates a cluster with given number of active nodes and the cluster manager
   */
  private List<AtomixReplica> createReplicas(int nodes, ClusterManager clusterManager) throws Throwable {
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      members.add(nextAddress());
    }

    this.members.addAll(members);
    List<AtomixReplica> replicas = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      AtomixReplica atomix = createReplica(members.get(i), AtomixReplica.Type.ACTIVE, clusterManager);
      atomix.bootstrap(members).thenRun(this::resume);
      replicas.add(atomix);
    }

    await(3000 * nodes, nodes);
    return replicas;
  }

  /**
   * Creates and joins replicas of given types to a cluster
   */
  private List<AtomixReplica> join(ClusterManager clusterManager, AtomixReplica.Type... types) throws Throwable {
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      members.add(nextAddress());
    }

    this.members.addAll(members);
    List<AtomixReplica> replicas = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      AtomixReplica atomix = createReplica(members.get(i), types[i], clusterManager);
      atomix.join(this.members).thenRun(this::resume);
      replicas.add(atomix);
    }

    await(3000 * types.length, types.length);
    return replicas;
  }

  private void init() {
    port = 5000;
    registry = new LocalServerRegistry();
    members = new ArrayList<>();
    replicas = new ArrayList<>();
  }

  private void cleanup() throws Throwable {
    for (AtomixReplica replica : replicas) {
      replica.server().server().leave().thenRun(this::resume);
    }

    await(3000 * replicas.size(), replicas.size());
    replicas.clear();
    members.clear();
  }
}
