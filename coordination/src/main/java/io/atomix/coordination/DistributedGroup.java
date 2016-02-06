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
import io.atomix.copycat.client.Command;
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
 * Provides a mechanism for managing group membership and remote scheduling and execution.
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
 * <b>Joining the group</b>
 * <p>
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
 * <b>Remote execution</b>
 * <p>
 * Once members of the group, any member can {@link GroupMember#execute(Runnable) execute} immediate callbacks or
 * {@link GroupMember#schedule(Duration, Runnable) schedule} delayed callbacks to be run on any other member of the
 * group. Submitting a {@link Runnable} callback to a member will cause it to be serialized and sent to that node
 * to be executed.
 * <pre>
 *   {@code
 *   group.onJoin(member -> {
 *     long memberId = member.id();
 *     member.execute((Serializable & Runnable) () -> System.out.println("Executing on " + memberId));
 *   });
 *   }
 * </pre>
 *
 * @see GroupMember
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-20, stateMachine=GroupState.class)
public class DistributedGroup extends Resource<DistributedGroup, Resource.Options> {
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
        if (this.leader.equals(leader)) {
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
   *
   * @return The current group leader.
   */
  public GroupMember leader() {
    return leader != null ? members.get(leader) : null;
  }

  /**
   * Returns the current group term.
   *
   * @return The current group term.
   */
  public long term() {
    return term;
  }

  /**
   * Registers a callback to be called when the term changes.
   *
   * @param callback The callback to be called when the term changes.
   * @return The term listener.
   */
  public Listener<Long> onTerm(Consumer<Long> callback) {
    return termListeners.add(callback);
  }

  /**
   * Registers a callback to be called when a member of the group is elected leader.
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
