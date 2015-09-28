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

import io.atomix.Resource;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.coordination.state.MembershipGroupCommands;
import io.atomix.coordination.state.MembershipGroupState;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.server.StateMachine;
import io.atomix.resource.ResourceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Distributed member group.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DistributedMembershipGroup extends Resource<DistributedMembershipGroup> {
  private final Listeners<GroupMember> joinListeners = new Listeners<>();
  private final Listeners<GroupMember> leaveListeners = new Listeners<>();
  private final Map<Long, GroupMember> members = new ConcurrentHashMap<>();

  @Override
  protected void open(ResourceContext context) {
    super.open(context);

    context.session().<Long>onEvent("join", memberId -> {
      GroupMember member = members.computeIfAbsent(memberId, m -> new InternalGroupMember(m));
      for (Listener<GroupMember> listener : joinListeners) {
        listener.accept(member);
      }
    });

    context.session().onEvent("leave", memberId -> {
      GroupMember member = members.remove(memberId);
      if (member != null) {
        for (Listener<GroupMember> listener : leaveListeners) {
          listener.accept(member);
        }
      }
    });

    context.session().onEvent("execute", Runnable::run);
  }

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return MembershipGroupState.class;
  }

  /**
   * Returns a member by ID.
   *
   * @param memberId The member ID.
   * @return The member.
   */
  public GroupMember member(long memberId) {
    return members.get(memberId);
  }

  /**
   * Returns the collection of members in the group.
   *
   * @return The collection of members in the group.
   */
  public Collection<GroupMember> members() {
    return members.values();
  }

  /**
   * Joins the member to the membership group.
   *
   * @return A completable future to be completed once the member has joined.
   */
  public CompletableFuture<Void> join() {
    return submit(MembershipGroupCommands.Join.builder().build());
  }

  /**
   * Adds a join listener.
   *
   * @param listener The join listener.
   * @return The listener context.
   */
  public Listener<GroupMember> onJoin(Consumer<GroupMember> listener) {
    return joinListeners.add(listener);
  }

  /**
   * Leaves the member from the membership group.
   *
   * @return A completable future to be completed once the member has left.
   */
  public CompletableFuture<Void> leave() {
    return submit(MembershipGroupCommands.Leave.builder().build());
  }

  /**
   * Adds a leave listener.
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

    /**
     * Returns the member ID.
     *
     * @return The member ID.
     */
    public long id() {
      return memberId;
    }

    /**
     * Schedules a callback to run at the given instant.
     *
     * @param instant The instant at which to run the callback.
     * @param callback The callback to run.
     * @return A completable future to be completed once the callback has been scheduled.
     */
    public CompletableFuture<Void> schedule(Instant instant, Runnable callback) {
      return schedule(instant.minusMillis(System.currentTimeMillis()), callback);
    }

    /**
     * Schedules a callback to run after the given delay on the member.
     *
     * @param delay The delay after which to run the callback.
     * @param callback The callback to run.
     * @return A completable future to be completed once the callback has been scheduled.
     */
    public CompletableFuture<Void> schedule(Duration delay, Runnable callback) {
      return submit(MembershipGroupCommands.Schedule.builder()
        .withMember(memberId)
        .withDelay(delay.toMillis())
        .withCallback(callback)
        .build());
    }

    /**
     * Executes a callback on the group member.
     *
     * @param callback The callback to execute.
     * @return A completable future to be completed once the callback has completed.
     */
    public CompletableFuture<Void> execute(Runnable callback) {
      return submit(MembershipGroupCommands.Execute.builder()
        .withMember(memberId)
        .withCallback(callback)
        .build());
    }
  }

}
