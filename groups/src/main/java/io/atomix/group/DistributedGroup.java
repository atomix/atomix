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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Listener;
import io.atomix.group.election.Election;
import io.atomix.group.election.Term;
import io.atomix.group.internal.DistributedGroupFactory;
import io.atomix.group.messaging.Message;
import io.atomix.group.messaging.MessageClient;
import io.atomix.group.messaging.MessageService;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Generic group abstraction for managing group membership, service discovery, leader election, and remote
 * scheduling and execution.
 * <p>
 * The distributed group resource facilitates managing group membership within an Atomix cluster. Membership is
 * managed by nodes {@link #join() joining} and {@link LocalMember#leave() leaving} the group, and instances
 * of the group throughout the cluster are notified on changes to the structure of the group. Groups can elect a
 * leader, and members can communicate directly with one another or through persistent queues.
 * <p>
 * Groups membership is managed in a replicated state machine. When a member joins the group, the join request
 * is replicated, the member is added to the group, and the state machine notifies instances of the
 * {@code DistributedGroup} of the membership change. In the event that a group instance becomes disconnected from
 * the cluster and its session times out, the replicated state machine will automatically remove the member
 * from the group and notify the remaining instances of the group of the membership change.
 * <p>
 * To create a membership group resource, use the {@code DistributedGroup} class or constructor:
 * <pre>
 *   {@code
 *   atomix.getGroup("my-group").thenAccept(group -> {
 *     ...
 *   });
 *   }
 * </pre>
 * <h2>Configuration</h2>
 * {@code DistributedGroup} instances can be configured to control {@link Member#messages() communication}
 * between members of the group. To configure groups, a {@link DistributedGroup.Options} instance must be provided
 * when constructing the initial group instance.
 * <p>
 * The {@link DistributedGroup.Options} define the configuration of the local {@code DistributedGroup} instance
 * only. The group options will <em>not</em> be replicated to or applied on any other node in the cluster. However,
 * group instances accessed via the {@code Atomix} API are static, so the options provided on the first instantiation
 * of a group will be used for the local instance until the resource is deleted.
 * <p>
 * The primary role of {@link DistributedGroup.Options} is configuring the group's method of communication with
 * other group instances around the cluster. In order to support direct messaging between members, the group
 * must be configured with an {@link Address} to which to bind the server for communication.
 * <pre>
 *   {@code
 *   DistributedGroup.Options options = new DistributedGroup.Options()
 *     .withAddress(new Address("localhost", 6000));
 *   DistributedGroup group = atomix.getGroup("message-group", options).get();
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
 * <h2>Persistent members</h2>
 * {@code DistributedGroup} supports a concept of persistent members that requires members to <em>explicitly</em>
 * {@link LocalMember#leave() leave} the group to be removed from it. Persistent member {@link Message tasks} will remain
 * in a failed member's queue until the member recovers.
 * <p>
 * In order to support recovery, persistent members must be configured with a user-provided {@link Member#id() member ID}.
 * The member ID is provided when the member {@link #join(String) joins} the group, and providing a member ID is
 * all that's required to create a persistent member.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("persistent-members").get();
 *   LocalGroupMember memberA = group.join("a").get();
 *   LocalGroupMember memberB = group.join("b").get();
 *   }
 * </pre>
 * Persistent members are not limited to a single node. If a node crashes, any persistent members that existed
 * on that node may rejoin the group on any other node. Persistent members rejoin simply by calling {@link #join(String)}
 * with the unique member ID. Once a persistent member has rejoined the group, its session will be updated and any
 * tasks remaining in the member's {@link MessageService} will be published to the member.
 * <p>
 * Persistent member state is retained <em>only</em> inside the group's replicated state machine and not on clients.
 * From the perspective of {@code DistributedGroup} instances in a cluster, in the event that the node on which
 * a persistent member is running fails, the member will {@link #onLeave(Consumer) leave} the group. Once the persistent
 * member rejoins the group, {@link #onJoin(Consumer)} will be called again on each group instance in the cluster.
 * <h2>Leader election</h2>
 * The {@code DistributedGroup} resource facilitates leader election which can be used to coordinate a group by
 * ensuring only a single member of the group performs some set of operations at any given time. Leader election
 * is a core concept of membership groups, and because leader election is a low-overhead process, leaders are
 * elected for each group automatically.
 * <p>
 * Leaders are elected using a fair policy. The first member to {@link #join() join} a group will always become the
 * initial group leader. Each unique leader in a group is associated with a {@link Election#term() term}. The term
 * represents a globally unique, monotonically increasing token that can be used for fencing. Users can listen for
 * changes in group terms and leaders with event listeners:
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("election-group").get();
 *   group.election().onElection(term -> {
 *     ...
 *   });
 *   }
 * </pre>
 * The {@link Term#term() term} is guaranteed to be unique for each {@link Term#leader() leader} and is
 * guaranteed to be monotonically increasing. Each instance of a group is guaranteed to see the same leader for the
 * same term, and no two leaders can ever exist in the same term. In that sense, the terminology and constraints of
 * leader election in Atomix borrow heavily from the Raft consensus algorithm that underlies it.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("election-group").get();
 *
 *   // Partition the group into three partitions
 *   PartitionGroup partitions = group.partition(3);
 *
 *   // Await a leader election in partition 0
 *   partitions.partitions().get(0).election().onElection(term -> {
 *     ...
 *   });
 *   }
 * </pre>
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
 * <h2>Direct messaging</h2>
 * Members of a group and group instances can communicate with one another through the direct messaging API,
 * {@link MessageService}. Direct messaging between group members is considered
 * <em>unreliable</em> and is done over the local node's configured {@link io.atomix.catalyst.transport.Transport}.
 * Messages between members of a group are ordered according only to the transport and are not guaranteed to be delivered.
 * While request-reply can be used to achieve some level of assurance that messages are delivered to specific members of
 * the group, direct message consumers should be idempotent and commutative.
 * <p>
 * In order to enable direct messaging for a group instance, the instance must be initialized with
 * {@link DistributedGroup.Options} that define the {@link Address} to which to bind a {@link Server} for messaging.
 * <pre>
 *   {@code
 *   DistributedGroup.Options options = new DistributedGroup.Options()
 *     .withAddress(new Address("localhost", 6000));
 *   DistributedGroup group = atomix.getGroup("message-group", options).get();
 *   }
 * </pre>
 * Once a group instance has been configured with an address for direct messaging, messages can be sent between
 * group members using the {@link MessageService} for any member of the group. Messages sent
 * between members must be associated with a {@link String} topic, and messages can be any value that is serializable by
 * the group instance's {@link io.atomix.catalyst.serializer.Serializer}.
 * <pre>
 *   {@code
 *   group.member("foo").connection().send("hello", "World!").thenAccept(reply -> {
 *     ...
 *   });
 *   }
 * </pre>
 * Direct messages can only be <em>received</em> by a {@link LocalMember} which must be created by
 * {@link #join() joining} the group. Local members register a listener for a link topic on the joined member's
 * {@link MessageService}. Message listeners are asynchronous. When a
 * {@link io.atomix.group.messaging.Message} is received by a local member, the member can perform any processing
 * it wishes and {@link io.atomix.group.messaging.Message#reply(Object) reply} to the message or
 * {@link io.atomix.group.messaging.Message#ack() acknowledge} completion of handling the message to send a response
 * back to the sender.
 * <pre>
 *   {@code
 *   // Join the group and run the given callback once successful
 *   group.join().thenAccept(member -> {
 *
 *     // Register a listener for the "hello" topic
 *     member.connection().onMessage("hello", message -> {
 *       // Handle the message and reply
 *       handleMessage(message);
 *       message.reply("Hello world!");
 *     });
 *
 *   });
 *   }
 * </pre>
 * It's critical that message listeners reply to messages, otherwise futures will be held in memory on the
 * sending side of the {@link MessageService} until the sender or receiver is removed from the
 * group.
 * <h3>Serialization</h3>
 * Users are responsible for ensuring the serializability of tasks, messages, and properties set on the group
 * and members of the group. Serialization is controlled by the group's {@link io.atomix.catalyst.serializer.Serializer}
 * which can be access via {@link #serializer()} or on the parent {@code Atomix} instance. Because objects are
 * typically replicated throughout the cluster, <em>it's critical that any object sent from any node should be
 * serializable by all other nodes</em>.
 * <p>
 * Users should register serializable types before performing any operations on the group.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("group").get();
 *   group.serializer().register(User.class, UserSerializer.class);
 *   }
 * </pre>
 * For the best performance from serialization, it is recommended that serializable types be registered with
 * unique type IDs. This allows the Catalyst {@link io.atomix.catalyst.serializer.Serializer} to identify the
 * type by its serialization ID rather than its class name. It's essential that the ID for a given type is
 * the same all all nodes in the cluster.
 * <pre>
 *   {@code
 *   group.serializer().register(User.class, 1, UserSerializer.class);
 *   }
 * </pre>
 * Users can also serialize {@link java.io.Serializable} types by simply registering the class without any
 * other serializer. Catalyst will attempt to use the optimal serializer based on the interfaces implemented
 * by the class. Alternatively, type registration can be disabled altogether via {@link Serializer#disableWhitelist()},
 * however this is not recommended as arbitrary deserialization of class names is slow and is a security risk.
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
 * @see Member
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-20, factory=DistributedGroupFactory.class)
public interface DistributedGroup extends Resource<DistributedGroup> {

  /**
   * Group configuration.
   */
  class Config extends Resource.Config {
    public Config() {
    }

    public Config(Properties defaults) {
      super(defaults);
    }

    /**
     * Sets the duration after which to remove persistent members from the group.
     *
     * @param expiration The duration after which to remove persistent members from the group.
     * @return The group configuration.
     * @throws NullPointerException if the expiration is {@code null}
     */
    public Config withMemberExpiration(Duration expiration) {
      setProperty("expiration", String.valueOf(Assert.notNull(expiration, "expiration").toMillis()));
      return this;
    }
  }

  /**
   * Group options.
   */
  class Options extends Resource.Options {
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

  @Override
  Config config();

  @Override
  Options options();

  /**
   * Returns the group election.
   * <p>
   * The returned election is specific to this group's set of members. The {@link Term} defined by the returned
   * election will not necessarily be reflected in any subgroups of this group.
   *
   * @return The group election.
   */
  Election election();

  /**
   * Returns the group message client.
   * <p>
   * The returned message client sends messages to all members of the group.
   *
   * @return The group message client.
   */
  MessageClient messages();

  /**
   * Gets a group member by ID.
   * <p>
   * If the member with the given ID has not {@link #join() joined} the membership group, the resulting
   * {@link Member} will be {@code null}.
   *
   * @param memberId The member ID for which to return a {@link Member}.
   * @return The member with the given {@code memberId} or {@code null} if it is not a known member of the group.
   */
  Member member(String memberId);

  /**
   * Gets the collection of all members in the group.
   * <p>
   * The group members are fetched from the cluster. If any {@link Member} instances have been referenced
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
  Collection<Member> members();

  /**
   * Joins the instance to the membership group.
   * <p>
   * When this instance joins the membership group, the membership lists of this and all other instances
   * in the group are guaranteed to be updated <em>before</em> the {@link CompletableFuture} returned by
   * this method is completed. Once this instance has joined the group, the returned future will be completed
   * with the {@link Member} instance for this member.
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
  CompletableFuture<LocalMember> join();

  /**
   * Joins the instance to the membership group with a user-provided member ID.
   * <p>
   * When this instance joins the membership group, the membership lists of this and all other instances
   * in the group are guaranteed to be updated <em>before</em> the {@link CompletableFuture} returned by
   * this method is completed. Once this instance has joined the group, the returned future will be completed
   * with the {@link Member} instance for this member.
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
  CompletableFuture<LocalMember> join(String memberId);

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
  Listener<Member> onJoin(Consumer<Member> listener);

  /**
   * Removes the member with the given member ID from the group.
   *
   * @param memberId The member ID of the member to remove from the group.
   * @return A completable future to be completed once the member has been removed.
   */
  CompletableFuture<Void> remove(String memberId);

  /**
   * Adds a listener for members leaving the group.
   * <p>
   * The provided {@link Consumer} will be called each time a member leaves the group. Members can
   * leave the group either voluntarily or by crashing or otherwise becoming disconnected from the
   * cluster for longer than their session timeout. Note that the leave consumer will be called before
   * the leaving member's {@link LocalMember#leave()} completes.
   * <p>
   * The returned {@link Listener} can be used to {@link Listener#close() unregister} the listener
   * when its use if finished.
   *
   * @param listener The leave listener.
   * @return The listener context.
   */
  Listener<Member> onLeave(Consumer<Member> listener);

}
