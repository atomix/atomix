/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.group;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.state.GroupCommands;
import io.atomix.group.util.DistributedGroupFactory;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Generic group abstraction for managing group membership, service discovery, leader election, and remote
 * scheduling and execution.
 * <p>
 * The distributed membership group resource facilitates managing group membership within the Atomix cluster.
 * Each instance of a {@code DistributedGroup} resource represents a single {@link GroupMember}.
 * Members can {@link #join()} and {@link LocalGroupMember#leave()} the group and be notified of other members
 * {@link #onJoin(Consumer) joining} and {@link #onLeave(Consumer) leaving} the group. Members may leave the group
 * either voluntarily or due to a failure or other disconnection from the cluster.
 * <p>
 * To create a membership group resource, use the {@code DistributedGroup} class or constructor:
 * <pre>
 *   {@code
 *   atomix.getGroup("my-group").thenAccept(group -> {
 *     ...
 *   });
 *   }
 * </pre>
 * <h2>Joining the group</h2>
 * When a new instance of the resource is created, it is initialized with an empty {@link #members()} list
 * as it is not yet a member of the group. Once the instance has been created, the user must join the group
 * via {@link #join()}:
 * <pre>
 *   {@code
 *   group.join().thenAccept(member -> {
 *     System.out.println("Joined with member ID: " + member.id());
 *   });
 *   }
 * </pre>
 * Once the group has been joined, the {@link #members()} list provides an up-to-date view of the group which will
 * be automatically updated as members join and leave the group. To be explicitly notified when a member joins or
 * leaves the group, use the {@link #onJoin(Consumer)} or {@link #onLeave(Consumer)} event consumers respectively:
 * <pre>
 *   {@code
 *   group.onJoin(member -> {
 *     System.out.println(member.id() + " joined the group!");
 *   });
 *   }
 * </pre>
 * <h2>Listing the members in the group</h2>
 * Users of the distributed group do not have to join the group to interact with it. For instance, while a server
 * may participate in the group by joining it, a client may interact with the group just to get a list of available
 * members. To access the list of group members, use the {@link #members()} getter:
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("foo").get();
 *   for (GroupMember member : group.members()) {
 *     ...
 *   }
 *   }
 * </pre>
 * Once the group instance has been created, the group membership will be automatically updated each time the structure
 * of the group changes. However, in the event that the client becomes disconnected from the cluster, it may not receive
 * notifications of changes in the group structure.
 * <h2>Leader election</h2>
 * The {@code DistributedGroup} resource facilitates leader election which can be used to coordinate a group by
 * ensuring only a single member of the group performs some set of operations at any given time. Leader election
 * is a core concept of membership groups, and because leader election is a low-overhead process, leaders are
 * elected for each group automatically.
 * <p>
 * Leaders are elected using a fair policy. The first member to {@link #join() join} a group will always become the
 * initial group leader. Each unique leader in a group is associated with a {@link GroupElection#term() term}. The term is a
 * globally unique, monotonically increasing token that can be used for fencing. Users can listen for changes in
 * group terms and leaders with event listeners:
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("foo").get();
 *   group.election().onTerm(term -> {
 *     ...
 *   });
 *   group.election().onElection(leader -> {
 *     ...
 *   });
 *   }
 * </pre>
 * The {@link GroupElection#term() term} is guaranteed to be incremented prior to the election of a new {@link GroupElection#leader() leader},
 * and only a single leader for any given term will ever be elected. Each instance of a group is guaranteed to see
 * terms and leaders progress monotonically, and no two leaders can exist in the same term. In that sense, the
 * terminology and constrains of leader election in Atomix borrow heavily from the Raft algorithm that underlies it.
 * <p>
 * While terms and leaders are guaranteed to progress in the same order from the perspective of all clients of
 * the resource, Atomix cannot guarantee that two leaders cannot exist at any given time. The group state machine
 * will make a best effort attempt to ensure that all clients are notified of a term or leader change prior to the
 * change being completed, but arbitrary process pauses due to garbage collection and other effects can cause a client's
 * session to expire and thus prevent the client from being updated in real time. Only clients that can maintain their
 * session are guaranteed to have a consistent view of the membership, term, and leader in the group at any given
 * time.
 * <p>
 * To guard against inconsistencies resulting from arbitrary process pauses, clients can use the monotonically
 * increasing term for coordination and managing optimistic access to external resources.
 * <h2>Consistent hashing</h2>
 * Membership groups also provide features to aid in supporting replication via consistent hashing and partitioning.
 * When a group is created, users can configure the group to support a particular number of partitions and replication
 * factor. Partitioning can aid in hashing resources to specific members of the group, and the replication factor builds
 * on partitions to aid in identifying multiple members per partition.
 * <p>
 * By default, groups are created with a single partition and replication factor of {@code 1}. To configure the group
 * for more partitions, provide a {@link DistributedGroup.Config} when creating the resource.
 * <pre>
 *   {@code
 *   DistributedGroup.Config config = DistributedGroup.config()
 *     .withPartitions(32)
 *     .withVirtualNodes(200)
 *     .withReplicationFactor(3);
 *   DistributedGroup group = atomix.getGroup("foo", config);
 *   }
 * </pre>
 * Partitions are managed within a consistent hash ring. For each member of the cluster, {@code 100} virtual nodes
 * are created on the ring by default. This helps spread reduce hotspotting within the ring. For each partition, the
 * partition is mapped to a set of members of the group by hashing the partition to a point on the ring. Once hashed
 * to a point on the ring, the {@code n} members following that point are the replicas for that partition.
 * <p>
 * Partition features are accessed via the group's {@link GroupPartitions} instance, which can be fetched via
 * {@link #partitions()}.
 * <pre>
 *   {@code
 *   group.partitions().partition(1).members().forEach(m -> ...);
 *   }
 * </pre>
 * Partitions change over time while members are added to or removed from the group. Each time a member is added or
 * removed, the group state machine will reassign the minimal number of partitions necessary to balance the cluster,
 * and {@code DistributedGroup} instances will be notified and updated automatically. Atomix guarantees that when a
 * new member {@link #join() joins} a group, all partition information on all connected group instances will be updated
 * before the join completes. Similarly, when a member {@link LocalGroupMember#leave() leaves} the group, all partition
 * information on all connected group instances are guaranteed to be updated before the operation completes.
 * <p>
 * Groups also aid in hashing objects to specific partitions and thus replicas within the group. Users can provide a
 * {@link GroupPartitioner} class in the {@link DistributedGroup.Options} when a group instance is first created on a node.
 * The partitioner will be used to determine the partition to which an object maps within the current set of partitions
 * when {@link GroupPartitions#partition(Object)} is called.
 * <pre>
 *   {@code
 *   group.partitions().partition("foo").members().forEach(m -> m.send("foo"));
 *   }
 * </pre>
 * <h2>Remote execution</h2>
 * Once members of the group, any member can {@link GroupScheduler#execute(Runnable) execute} immediate callbacks or
 * {@link GroupScheduler#schedule(Duration, Runnable) schedule} delayed callbacks to be run on any other member of the
 * group. Submitting a {@link Runnable} callback to a member will cause it to be serialized and sent to that node
 * to be executed.
 * <pre>
 *   {@code
 *   group.onJoin(member -> {
 *     String memberId = member.id();
 *     member.execute((Serializable & Runnable) () -> System.out.println("Executing on " + memberId));
 *   });
 *   }
 * </pre>
 * <h3>Implementation</h3>
 * Group state is managed in a Copycat replicated {@link io.atomix.copycat.server.StateMachine}. When a
 * {@code DistributedGroup} is created, an instance of the group state machine is created on each replica in
 * the cluster. The state machine instance manages state for the specific membership group. When a member
 * {@link #join() joins} the group, a join request is sent to the cluster and logged and replicated before
 * being applied to the group state machine. Once the join request has been committed and applied to the
 * state machine, the group state is updated and existing group members are notified by
 * {@link io.atomix.copycat.server.session.ServerSession#publish(String, Object) publishing} state change
 * notifications to open instances of the group. Membership change event notifications are received by all
 * open instances of the resource.
 * <p>
 * Leader election is performed by the group state machine. When the first member joins the group, that
 * member will automatically be assigned as the group member. Each time an additional member joins the group,
 * the new member will be placed in a leader queue. In the event that the current group leader's
 * {@link io.atomix.copycat.session.Session} expires or is closed, the group state machine will assign a new
 * leader by pulling from the leader queue and will publish an {@code elect} event to all remaining group
 * members. Additionally, for each new leader of the group, the state machine will publish a {@code term} change
 * event, providing a globally unique, monotonically increasing token uniquely associated with the new leader.
 * <p>
 * To track group membership, the group state machine tracks the state of the {@link io.atomix.copycat.session.Session}
 * associated with each open instance of the group. In the event that the session expires or is closed, the group
 * member associated with that session will automatically be removed from the group and remaining instances
 * of the group will be notified.
 * <p>
 * Partitions are determined by consistent hashing using the group's members and the configured partition strategy.
 * Each time a member is added to the group, the member is added to a consistent hash ring. When a member leaves the
 * group, the member is removed from the ring. Partitions are mapped to a point on the ring, and the {@code n} members
 * following that point are the replicas for that partition. Virtual nodes are added to the ring to balance the
 * membership.
 * <p>
 * The group state machine facilitates messaging and remote execution by routing serialize messages and callbacks
 * to specific members of the group by publishing event messages to the desired resource session. Messages and
 * callbacks are logged and replicated like any other state change in the group. Once a message or callback has
 * been successfully published and received by the appropriate client, the associated commit is released from
 * the state machine and will be removed from the log during compaction.
 * <p>
 * The group state machine manages compaction of the replicated log by tracking which state changes contribute to
 * the state of the group at any given time. For instance, when a member joins the group, the commit that added the
 * member to the group contributes to the group's state as long as the member remains a part of the group. Once the
 * member leaves the group or its session is expired, the commit that created and remove the member no longer contribute
 * to the group's state and are therefore released from the state machine and will be removed from the log during
 * compaction.
 *
 * @see GroupMember
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-20, factory=DistributedGroupFactory.class)
public class DistributedGroup extends AbstractResource<DistributedGroup> {

  /**
   * Group configuration.
   */
  public static class Config extends Resource.Config {
    public Config() {
    }

    public Config(Properties defaults) {
      super(defaults);
    }

    /**
     * Sets the hash function with which to hash keys.
     *
     * @param hasherClass The hasher function class.
     * @return The group configuration.
     */
    public Config withHasher(Class<? extends Hasher> hasherClass) {
      setProperty("hasher", hasherClass.getName());
      return this;
    }

    /**
     * Sets the group partitioner class.
     *
     * @param partitionerClass The group partitioner class.
     * @return The group configuration.
     */
    public Config withPartitioner(Class<? extends GroupPartitioner> partitionerClass) {
      setProperty("partitioner", partitionerClass.getName());
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param partitions The number of partitions.
     * @return The group configuration.
     */
    public Config withPartitions(int partitions) {
      setProperty("partitions", String.valueOf(partitions));
      return this;
    }

    /**
     * Sets the number of virtual nodes per group member in the consistent hash ring.
     *
     * @param virtualNodes The number of virtual members per group member.
     * @return The group configuration.
     */
    public Config withVirtualNodes(int virtualNodes) {
      setProperty("virtualNodes", String.valueOf(virtualNodes));
      return this;
    }

    /**
     * Sets the group replication factor.
     *
     * @param replicationFactor The group replication factor.
     * @return The group configuration.
     */
    public Config withReplicationFactor(int replicationFactor) {
      setProperty("replicationFactor", String.valueOf(replicationFactor));
      return this;
    }
  }

  /**
   * Group options.
   */
  public static class Options extends Resource.Options {
    private static final String ADDRESS = "address";

    public Options() {
    }

    public Options(Properties defaults) {
      super(defaults);
    }

    /**
     * Returns the message bus address.
     *
     * @return The message bus address.
     */
    public Address getAddress() {
      String addressString = getProperty(ADDRESS);
      if (addressString == null)
        return null;

      String[] split = addressString.split(":");
      if (split.length != 2)
        throw new ConfigurationException("malformed address string: " + addressString);

      try {
        return new Address(split[0], Integer.valueOf(split[1]));
      } catch (NumberFormatException e) {
        throw new ConfigurationException("malformed port: " + split[1]);
      }
    }

    /**
     * Sets the local message bus server address.
     *
     * @param address The local message bus server address.
     * @return The message bus options.
     */
    public Options withAddress(Address address) {
      setProperty(ADDRESS, String.format("%s:%s", address.host(), address.port()));
      return this;
    }
  }

  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final Set<String> joining = new CopyOnWriteArraySet<>();
  private final Address address;
  private final Server server;
  final GroupConnectionManager connections;
  private final GroupProperties properties = new GroupProperties(null, this);
  private final GroupElection election = new GroupElection(this);
  private final GroupScheduler scheduler = new GroupScheduler(null, this);
  private final GroupTaskQueue tasks = new GroupTaskQueue(null, this);
  private GroupHashRing hashRing;
  private final GroupPartitions partitions = new GroupPartitions();
  final Map<String, GroupMember> members = new ConcurrentHashMap<>();

  public DistributedGroup(CopycatClient client, Properties options) {
    super(client, options);
    this.address = new Options(options).getAddress();
    this.server = client.transport().server();
    this.connections = new GroupConnectionManager(client.transport().client(), client.context());
  }

  @Override
  public Options options() {
    return new Options(super.options());
  }

  /**
   * Returns the group properties.
   *
   * @return The group properties.
   */
  public GroupProperties properties() {
    return properties;
  }

  /**
   * Returns the group election.
   *
   * @return The group election.
   */
  public GroupElection election() {
    return election;
  }

  /**
   * Returns the group task queue.
   *
   * @return The group task queue.
   */
  public GroupTaskQueue tasks() {
    return tasks;
  }

  /**
   * Returns the group scheduler.
   *
   * @return The group scheduler.
   */
  public GroupScheduler scheduler() {
    return scheduler;
  }

  /**
   * Returns the group partitions.
   *
   * @return The group partitions.
   */
  public GroupPartitions partitions() {
    return partitions;
  }

  /**
   * Gets a group member by ID.
   * <p>
   * If the member with the given ID has not {@link #join() joined} the membership group, the resulting
   * {@link GroupMember} will be {@code null}.
   *
   * @param memberId The member ID for which to return a {@link GroupMember}.
   * @return The member with the given {@code memberId} or {@code null} if it is not a known member of the group.
   */
  public GroupMember member(String memberId) {
    return members.get(memberId);
  }

  /**
   * Gets the collection of all members in the group.
   * <p>
   * The group members are fetched from the cluster. If any {@link GroupMember} instances have been referenced
   * by this membership group instance, the same object will be returned for that member.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   Collection<GroupMember> members = group.members().get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   group.members().thenAccept(members -> {
   *     members.forEach(member -> {
   *       member.send("test", "Hello world!");
   *     });
   *   });
   *   }
   * </pre>
   *
   * @return The collection of all members in the group.
   */
  public Collection<GroupMember> members() {
    return members.values();
  }

  /**
   * Joins the instance to the membership group.
   * <p>
   * When this instance joins the membership group, the membership lists of this and all other instances
   * in the group are guaranteed to be updated <em>before</em> the {@link CompletableFuture} returned by
   * this method is completed. Once this instance has joined the group, the returned future will be completed
   * with the {@link GroupMember} instance for this member.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   group.join().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   group.join().thenAccept(thisMember -> System.out.println("This member is: " + thisMember.id()));
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the member has joined.
   */
  public CompletableFuture<LocalGroupMember> join() {
    return join(UUID.randomUUID().toString(), false);
  }

  /**
   * Joins the instance to the membership group with a user-provided member ID.
   * <p>
   * When this instance joins the membership group, the membership lists of this and all other instances
   * in the group are guaranteed to be updated <em>before</em> the {@link CompletableFuture} returned by
   * this method is completed. Once this instance has joined the group, the returned future will be completed
   * with the {@link GroupMember} instance for this member.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   group.join().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   group.join().thenAccept(thisMember -> System.out.println("This member is: " + thisMember.id()));
   *   }
   * </pre>
   *
   * @param memberId The unique member ID to assign to the member.
   * @return A completable future to be completed once the member has joined.
   */
  public CompletableFuture<LocalGroupMember> join(String memberId) {
    return join(memberId, true);
  }

  /**
   * Joins the group.
   *
   * @param memberId The member ID with which to join the group.
   * @param persistent Indicates whether the member ID is persistent.
   * @return A completable future to be completed once the member has joined the group.
   */
  private CompletableFuture<LocalGroupMember> join(String memberId, boolean persistent) {
    joining.add(memberId);
    return submit(new GroupCommands.Join(memberId, address, persistent)).whenComplete((result, error) -> {
      if (error != null) {
        joining.remove(memberId);
      }
    }).thenApply(info -> {
      LocalGroupMember member = (LocalGroupMember) members.get(info.memberId());
      if (member == null) {
        member = new LocalGroupMember(info, this);
        members.put(info.memberId(), member);
      }
      return member;
    });
  }

  /**
   * Adds a listener for members joining the group.
   * <p>
   * The provided {@link Consumer} will be called each time a member joins the group. Note that
   * the join consumer will be called before the joining member's {@link #join()} completes.
   * <p>
   * The returned {@link Listener} can be used to {@link Listener#close() unregister} the listener
   * when its use if finished.
   *
   * @param listener The join listener.
   * @return The listener context.
   */
  public Listener<GroupMember> onJoin(Consumer<GroupMember> listener) {
    return joinListeners.add(listener);
  }

  /**
   * Adds a listener for members leaving the group.
   * <p>
   * The provided {@link Consumer} will be called each time a member leaves the group. Members can
   * leave the group either voluntarily or by crashing or otherwise becoming disconnected from the
   * cluster for longer than their session timeout. Note that the leave consumer will be called before
   * the leaving member's {@link LocalGroupMember#leave()} completes.
   * <p>
   * The returned {@link Listener} can be used to {@link Listener#close() unregister} the listener
   * when its use if finished.
   *
   * @param listener The leave listener.
   * @return The listener context.
   */
  public Listener<GroupMember> onLeave(Consumer<GroupMember> listener) {
    return leaveListeners.add(listener);
  }

  @Override
  public CompletableFuture<DistributedGroup> open() {
    return super.open().thenApply(result -> {
      // Configure the hasher.
      Hasher hasher;
      String hasherClass = config.getProperty("hasher", Murmur2Hasher.class.getName());
      try {
        hasher = (Hasher) Thread.currentThread().getContextClassLoader().loadClass(hasherClass).newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }

      // Set up the hash ring.
      this.hashRing = new GroupHashRing(hasher, Integer.valueOf(config.getProperty("virtualNodes", "100")), Integer.valueOf(config.getProperty("replicationFactor", "1")));

      // Configure the partitioner.
      String partitionerClass = config.getProperty("partitioner", HashPartitioner.class.getName());
      try {
        this.partitions.partitioner = (GroupPartitioner) Thread.currentThread().getContextClassLoader().loadClass(partitionerClass).newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }

      // Set up the empty partitions.
      int partitions = Integer.valueOf(config.getProperty("partitions", "1"));
      for (int i = 0; i < partitions; i++) {
        this.partitions.partitions.add(new GroupPartition(i));
      }

      // Populate the partitions with any existing group members.
      updatePartitions();

      client.onEvent("join", this::onJoinEvent);
      client.onEvent("leave", this::onLeaveEvent);
      client.onEvent("term", election::onTermEvent);
      client.onEvent("elect", election::onElectEvent);
      client.onEvent("resign", election::onResignEvent);
      client.onEvent("task", this::onTaskEvent);
      client.onEvent("ack", this::onAckEvent);
      client.onEvent("fail", this::onFailEvent);
      client.onEvent("execute", this::onExecuteEvent);

      return result;
    }).thenCompose(v -> listen())
      .thenCompose(v -> sync())
      .thenApply(v -> this);
  }

  /**
   * Starts the server.
   */
  private CompletableFuture<Void> listen() {
    if (address != null) {
      return server.listen(address, c -> {
        c.handler(GroupMessage.class, this::onMessage);
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Handles a group message.
   */
  private CompletableFuture<Object> onMessage(GroupMessage message) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    GroupMember member = members.get(message.member());
    if (member != null) {
      if (member instanceof LocalGroupMember) {
        ((LocalGroupMember) member).connection().handleMessage(message.setFuture(future));
      } else {
        future.completeExceptionally(new IllegalStateException("not a local member"));
      }
    } else {
      future.completeExceptionally(new IllegalStateException("unknown member"));
    }
    return future;
  }

  /**
   * Synchronizes the membership group.
   */
  private CompletableFuture<Void> sync() {
    return submit(new GroupCommands.Listen()).thenAccept(members -> {
      for (GroupMemberInfo info : members) {
        this.members.computeIfAbsent(info.memberId(), m -> new GroupMember(info, this));
      }
    });
  }

  /**
   * Converts an integer to a byte array.
   */
  private byte[] intToByteArray(int value) {
    return new byte[]{(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
  }

  /**
   * Updates group partitions.
   */
  private void updatePartitions() {
    for (int i = 0; i < this.partitions.partitions.size(); i++) {
      this.partitions.partitions.get(i).handleRepartition(hashRing.members(intToByteArray(i)));
    }
  }

  /**
   * Handles a join event received from the cluster.
   */
  private void onJoinEvent(GroupMemberInfo info) {
    GroupMember member;
    if (joining.contains(info.memberId())) {
      member = new LocalGroupMember(info, this);
      if (members.containsKey(info.memberId())) {
        hashRing.removeMember(member);
      }
      members.put(info.memberId(), member);
      hashRing.addMember(member);
    } else {
      member = members.get(info.memberId());
      if (member == null) {
        member = new GroupMember(info, this);
        members.put(info.memberId(), member);
        hashRing.addMember(member);
      }
    }

    updatePartitions();

    for (Listener<GroupMember> listener : joinListeners) {
      listener.accept(member);
    }
  }

  /**
   * Handles a leave event received from the cluster.
   */
  private void onLeaveEvent(String memberId) {
    GroupMember member = members.remove(memberId);
    if (member != null) {
      hashRing.removeMember(member);
      updatePartitions();
      for (Listener<GroupMember> listener : leaveListeners) {
        listener.accept(member);
      }
    }
  }

  /**
   * Handles a task event received from the cluster.
   */
  private void onTaskEvent(GroupTask task) {
    GroupMember localMember = members.get(task.member());
    if (localMember != null && localMember instanceof LocalGroupMember) {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      future.whenComplete((succeeded, error) -> {
        if (error == null && succeeded) {
          submit(new GroupCommands.Ack(task.id(), task.member(), true));
        } else {
          submit(new GroupCommands.Ack(task.id(), task.member(), false));
        }
      });
      ((LocalGroupMember) localMember).tasks().handleTask(task.setFuture(future));
    }
  }

  /**
   * Handles an ack event received from the cluster.
   */
  private void onAckEvent(GroupCommands.Submit submit) {
    if (submit.member() != null) {
      GroupMember member = members.get(submit.member());
      if (member != null) {
        member.tasks().handleAck(submit.id());
      }
    } else {
      tasks.handleAck(submit.id());
    }
  }

  /**
   * Handles a fail event received from the cluster.
   */
  private void onFailEvent(GroupCommands.Submit submit) {
    if (submit.member() != null) {
      GroupMember member = members.get(submit.member());
      if (member != null) {
        member.tasks().handleFail(submit.id());
      }
    } else {
      tasks.handleFail(submit.id());
    }
  }

  /**
   * Handles an execute event received from the cluster.
   */
  private void onExecuteEvent(Runnable callback) {
    callback.run();
  }

  @Override
  protected <T> CompletableFuture<T> submit(Query<T> query) {
    return super.submit(query);
  }

  @Override
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return super.submit(command);
  }

}
