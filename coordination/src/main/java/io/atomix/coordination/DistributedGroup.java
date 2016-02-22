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
package io.atomix.coordination;

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.coordination.state.GroupCommands;
import io.atomix.coordination.state.GroupState;
import io.atomix.copycat.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
 * initial group leader. Each unique leader in a group is associated with a {@link #term() term}. The term is a
 * globally unique, monotonically increasing token that can be used for fencing. Users can listen for changes in
 * group terms and leaders with event listeners:
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("foo").get();
 *   group.onTerm(term -> {
 *     ...
 *   });
 *   group.onElection(leader -> {
 *     ...
 *   });
 *   }
 * </pre>
 * The {@link #term() term} is guaranteed to be incremented prior to the election of a new {@link #leader() leader},
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
 * <h2>Remote execution</h2>
 * Once members of the group, any member can {@link GroupMember#execute(Runnable) execute} immediate callbacks or
 * {@link GroupMember#schedule(Duration, Runnable) schedule} delayed callbacks to be run on any other member of the
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
@ResourceTypeInfo(id=-20, stateMachine=GroupState.class, typeResolver=GroupCommands.TypeResolver.class)
public class DistributedGroup extends Resource<DistributedGroup> {

  /**
   * Returns new group options.
   *
   * @return New group options.
   */
  public static Options options() {
    return new Options();
  }

  /**
   * Returns a new group configuration.
   *
   * @return A new group configuration.
   */
  public static Config config() {
    return new Config();
  }

  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final Listeners<Long> termListeners = new Listeners<>();
  private final Listeners<GroupMember> electionListeners = new Listeners<>();
  private final Map<String, InternalLocalGroupMember> localMembers = new ConcurrentHashMap<>();
  private final Map<String, GroupMember> members = new ConcurrentHashMap<>();
  private volatile String leader;
  private volatile long term;

  public DistributedGroup(CopycatClient client, Resource.Options options) {
    super(client, options);
  }

  @Override
  public CompletableFuture<DistributedGroup> open() {
    return super.open().thenApply(result -> {
      client.<String>onEvent("join", memberId -> {
        GroupMember member = members.computeIfAbsent(memberId, InternalGroupMember::new);
        for (Listener<GroupMember> listener : joinListeners) {
          listener.accept(member);
        }
      });

      client.<String>onEvent("leave", memberId -> {
        GroupMember member = members.remove(memberId);
        if (member != null) {
          for (Listener<GroupMember> listener : leaveListeners) {
            listener.accept(member);
          }
        }
      });

      client.<Long>onEvent("term", term -> {
        this.term = term;
        termListeners.accept(term);
      });

      client.<String>onEvent("elect", leader -> {
        this.leader = leader;
        electionListeners.accept(member(leader));
        InternalLocalGroupMember member = localMembers.get(leader);
        if (member != null) {
          member.electionListeners.accept(term);
        }
      });

      client.<String>onEvent("resign", leader -> {
        if (this.leader != null && this.leader.equals(leader)) {
          this.leader = null;
        }
      });

      client.<GroupCommands.Message>onEvent("message", message -> {
        InternalLocalGroupMember localMember = localMembers.get(message.member());
        if (localMember != null) {
          localMember.handle(message);
        }
      });

      client.onEvent("execute", Runnable::run);

      return result;
    }).thenCompose(v -> sync())
      .thenApply(v -> this);
  }

  /**
   * Synchronizes the membership group.
   */
  private CompletableFuture<Void> sync() {
    return submit(new GroupCommands.Listen()).thenAccept(members -> {
      for (String memberId : members) {
        this.members.computeIfAbsent(memberId, InternalGroupMember::new);
      }
    });
  }

  /**
   * Returns the current group leader.
   * <p>
   * The returned leader is the last known leader for the group. The leader is associated with
   * the current {@link #term()} which is guaranteed to be unique and monotonically increasing.
   * All resource instances are guaranteed to see leader changes in the same order. If a leader
   * leaves the group, it is guaranteed that all open resource instances are notified of the change
   * in leadership prior to the leave operation being completed. This guarantee is maintained only
   * as long as the resource's session remains open.
   * <p>
   * The leader is <em>not</em> guaranteed to be consistent across the cluster at any given point
   * in time. For example, a long garbage collection pause can result in the resource's session expiring
   * and the resource failing to increment the leader at the appropriate time. Users should use
   * the {@link #term()} for fencing when interacting with external systems.
   *
   * @return The current group leader.
   */
  public GroupMember leader() {
    return leader != null ? members.get(leader) : null;
  }

  /**
   * Returns the current group term.
   * <p>
   * The term is a globally unique, monotonically increasing token that represents an epoch.
   * All resource instances are guaranteed to see term changes in the same order. If a leader
   * leaves the group, it is guaranteed that the term will be incremented and all open resource
   * instances are notified of the term change prior to the leave operation being completed. However,
   * this guarantee is maintained only as long as the resource's session remains open.
   * <p>
   * For any given term, the group guarantees that a single {@link #leader()} will be elected
   * and any leader elected after the leader for this term will be associated with a higher
   * term.
   * <p>
   * The term is <em>not</em> guaranteed to be unique across the cluster at any given point in time.
   * For example, a long garbage collection pause can result in the resource's session expiring and the
   * resource failing to increment the term at the appropriate time. Users should use the term for
   * fencing when interacting with external systems.
   *
   * @return The current group term.
   */
  public long term() {
    return term;
  }

  /**
   * Registers a callback to be called when the term changes.
   * <p>
   * The provided callback will be called when a term change notification is received by the resource.
   * The returned {@link Listener} can be used to unregister the term listener via {@link Listener#close()}.
   *
   * @param callback The callback to be called when the term changes.
   * @return The term listener.
   */
  public Listener<Long> onTerm(Consumer<Long> callback) {
    return termListeners.add(callback);
  }

  /**
   * Registers a callback to be called when a member of the group is elected leader.
   * <p>
   * The provided callback will be called when notification of a leader change is received by the resource.
   * The returned {@link Listener} can be used to unregister the term listener via {@link Listener#close()}.
   *
   * @param callback The callback to call when a member of the group is elected leader.
   * @return The leader election listener.
   */
  public Listener<GroupMember> onElection(Consumer<GroupMember> callback) {
    return electionListeners.add(callback);
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
    return submit(new GroupCommands.Join(UUID.randomUUID().toString(), false)).thenApply(memberId -> {
      InternalLocalGroupMember member = new InternalLocalGroupMember(memberId);
      localMembers.put(member.id(), member);
      return member;
    });
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
    return submit(new GroupCommands.Join(memberId, true)).thenApply(id -> {
      InternalLocalGroupMember member = new InternalLocalGroupMember(id);
      localMembers.put(member.id(), member);
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
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return super.submit(command);
  }

  /**
   * Internal local group member.
   */
  private class InternalLocalGroupMember extends InternalGroupMember implements LocalGroupMember {
    private final Map<String, ListenerHolder> listeners = new ConcurrentHashMap<>();
    private final Listeners<Long> electionListeners = new Listeners<>();

    InternalLocalGroupMember(String memberId) {
      super(memberId);
    }

    @Override
    public CompletableFuture<Void> set(String property, Object value) {
      return submit(new GroupCommands.SetProperty(memberId, property, value));
    }

    @Override
    public CompletableFuture<Void> remove(String property) {
      return submit(new GroupCommands.RemoveProperty(memberId, property));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Listener<T> onMessage(String topic, Consumer<T> consumer) {
      ListenerHolder listener = new ListenerHolder(consumer);
      listeners.put(topic, listener);
      return listener;
    }

    /**
     * Handles a message to the member.
     */
    private void handle(GroupCommands.Message message) {
      ListenerHolder listener = listeners.get(message.topic());
      if (listener != null) {
        listener.accept(message.body());
      }
    }

    @Override
    public Listener<Long> onElection(Consumer<Long> callback) {
      Listener<Long> listener = electionListeners.add(callback);
      if (isLeader()) {
        listener.accept(term);
      }
      return listener;
    }

    @Override
    public CompletableFuture<Void> resign() {
      return submit(new GroupCommands.Resign(memberId));
    }

    @Override
    public CompletableFuture<Void> leave() {
      return submit(new GroupCommands.Leave(memberId)).whenComplete((result, error) -> {
        localMembers.remove(memberId);
      });
    }

    /**
     * Listener holder.
     */
    @SuppressWarnings("unchecked")
    private class ListenerHolder implements Listener {
      private final Consumer consumer;

      private ListenerHolder(Consumer consumer) {
        this.consumer = consumer;
      }

      @Override
      public void accept(Object message) {
        consumer.accept(message);
      }

      @Override
      public void close() {
        listeners.remove(this);
      }
    }
  }

  /**
   * Internal group member.
   */
  private class InternalGroupMember implements GroupMember {
    protected final String memberId;

    InternalGroupMember(String memberId) {
      this.memberId = memberId;
    }

    @Override
    public String id() {
      return memberId;
    }

    @Override
    public boolean isLeader() {
      return leader != null && leader.equals(memberId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> get(String property) {
      return submit(new GroupCommands.GetProperty(memberId, property)).thenApply(result -> (T) result);
    }

    @Override
    public CompletableFuture<Void> send(String topic, Object message) {
      return submit(new GroupCommands.Send(memberId, topic, message));
    }

    @Override
    public CompletableFuture<Void> schedule(Instant instant, Runnable callback) {
      return schedule(Duration.ofMillis(instant.toEpochMilli() - System.currentTimeMillis()), callback);
    }

    @Override
    public CompletableFuture<Void> schedule(Duration delay, Runnable callback) {
      return submit(new GroupCommands.Schedule(memberId, delay.toMillis(), callback));
    }

    @Override
    public CompletableFuture<Void> execute(Runnable callback) {
      return submit(new GroupCommands.Execute(memberId, callback));
    }
    
    @Override
    public String toString() {
      return memberId;
    }
  }

}
