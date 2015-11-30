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
import io.atomix.coordination.state.MembershipGroupCommands;
import io.atomix.coordination.state.MembershipGroupState;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.RaftClient;
import io.atomix.resource.Consistency;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Provides a mechanism for managing group membership and remote scheduling and execution.
 * <p>
 * The distributed membership group resource facilitates managing group membership within the Atomix cluster.
 * Each instance of a {@code DistributedMembershipGroup} resource represents a single {@link GroupMember}.
 * Members can {@link #join()} and {@link #leave()} the group and be notified of other members {@link #onJoin(Consumer) joining}
 * and {@link #onLeave(Consumer) leaving} the group. Members may leave the group either voluntarily or due to
 * a failure or other disconnection from the cluster.
 * <p>
 * To create a membership group resource, use the {@code DistributedMembershipGroup} class or constructor:
 * <pre>
 *   {@code
 *   atomix.create("group", DistributedMembershipGroup.class).thenAccept(group -> {
 *     ...
 *   });
 *   }
 * </pre>
 * <b>Joining the group</b>
 * <p>
 * When a new instance of the resource is created, it is initialized with an empty {@link #members()} list and
 * {@link #member()} as it is not yet a member of the group. Once the instance has been created, the instance must
 * join the group via {@link #join()}:
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
@ResourceInfo(stateMachine=MembershipGroupState.class)
public class DistributedMembershipGroup extends Resource {
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private GroupMember member;
  private final Map<Long, GroupMember> members = new ConcurrentHashMap<>();

  public DistributedMembershipGroup(RaftClient client) {
    super(client);

    client.session().<Long>onEvent("join", memberId -> {
      GroupMember member = members.computeIfAbsent(memberId, InternalGroupMember::new);
      for (Listener<GroupMember> listener : joinListeners) {
        listener.accept(member);
      }
    });

    client.session().<Long>onEvent("leave", memberId -> {
      GroupMember member = members.remove(memberId);
      if (member != null) {
        for (Listener<GroupMember> listener : leaveListeners) {
          listener.accept(member);
        }
      }
    });

    client.session().onEvent("execute", Runnable::run);
  }

  @Override
  public DistributedMembershipGroup with(Consistency consistency) {
    super.with(consistency);
    return this;
  }

  /**
   * Returns the local group member.
   * <p>
   * The local {@link GroupMember} is constructed when this instance {@link #join() joins} the group.
   * The {@link GroupMember#id()} is guaranteed to be unique to this instance throughout the lifetime of
   * this distributed resource.
   *
   * @return The local group member or {@code null} if the member has not joined the group.
   */
  public GroupMember member() {
    return member;
  }

  /**
   * Returns a group member by ID.
   *
   * @param memberId The member ID for which to return a {@link GroupMember}.
   * @return The member with the given {@code memberId} or {@code null} if it is not a known member of the group.
   */
  public GroupMember member(long memberId) {
    return members.get(memberId);
  }

  /**
   * Returns the collection of all members in the group.
   * <p>
   * The returned members collection is guaranteed to be representative of the current group state.
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
  public CompletableFuture<GroupMember> join() {
    return submit(new MembershipGroupCommands.Join()).thenApply(members -> {
      member = new InternalGroupMember(client.session().id());
      for (long memberId : members) {
        this.members.computeIfAbsent(memberId, InternalGroupMember::new);
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
   * Leaves the membership group.
   * <p>
   * When this instance leaves the membership group, the membership lists of this and all other instances
   * in the group are guaranteed to be updated <em>before</em> the {@link CompletableFuture} returned by
   * this method is completed. Once this instance has left the group, the returned future will be completed.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   group.leave().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   group.leave().thenRun(() -> System.out.println("Left the group!")));
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the member has left.
   */
  public CompletableFuture<Void> leave() {
    return submit(new MembershipGroupCommands.Leave()).whenComplete((result, error) -> {
      member = null;
      members.clear();
    });
  }

  /**
   * Adds a listener for members leaving the group.
   * <p>
   * The provided {@link Consumer} will be called each time a member leaves the group. Members can
   * leave the group either voluntarily or by crashing or otherwise becoming disconnected from the
   * cluster for longer than their session timeout. Note that the leave consumer will be called before
   * the leaving member's {@link #leave()} completes.
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
   * Internal group member.
   */
  private class InternalGroupMember implements GroupMember {
    private final long memberId;

    InternalGroupMember(long memberId) {
      this.memberId = memberId;
    }

    @Override
    public long id() {
      return memberId;
    }

    @Override
    public CompletableFuture<Void> schedule(Instant instant, Runnable callback) {
      return schedule(Duration.ofMillis(instant.toEpochMilli() - System.currentTimeMillis()), callback);
    }

    @Override
    public CompletableFuture<Void> schedule(Duration delay, Runnable callback) {
      return submit(new MembershipGroupCommands.Schedule(memberId, delay.toMillis(), callback));
    }

    @Override
    public CompletableFuture<Void> execute(Runnable callback) {
      return submit(new MembershipGroupCommands.Execute(memberId, callback));
    }
  }

}
