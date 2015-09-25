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
package io.atomix.copycat.coordination;

import io.atomix.catalog.client.Command;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.copycat.Resource;
import io.atomix.copycat.coordination.state.GroupCommands;
import io.atomix.copycat.coordination.state.GroupState;
import io.atomix.copycat.resource.ResourceContext;

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
public class DistributedGroup extends Resource {
  private final Listeners<Member> joinListeners = new Listeners<>();
  private final Listeners<Member> leaveListeners = new Listeners<>();
  private final Map<Long, Member> members = new ConcurrentHashMap<>();

  @Override
  protected void open(ResourceContext context) {
    super.open(context);

    context.session().<Long>onEvent("join", memberId -> {
      Member member = members.computeIfAbsent(memberId, m -> new Member(m, this));
      for (Listener<Member> listener : joinListeners) {
        listener.accept(member);
      }
    });

    context.session().onEvent("leave", memberId -> {
      Member member = members.remove(memberId);
      if (member != null) {
        for (Listener<Member> listener : leaveListeners) {
          listener.accept(member);
        }
      }
    });

    context.session().onEvent("execute", Runnable::run);
  }

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return GroupState.class;
  }

  /**
   * Returns a member by ID.
   *
   * @param memberId The member ID.
   * @return The member.
   */
  public Member member(long memberId) {
    return members.get(memberId);
  }

  /**
   * Returns the collection of members in the group.
   *
   * @return The collection of members in the group.
   */
  public Collection<Member> members() {
    return members.values();
  }

  /**
   * Joins the member to the membership group.
   *
   * @return A completable future to be completed once the member has joined.
   */
  public CompletableFuture<Void> join() {
    return submit(GroupCommands.Join.builder().build());
  }

  /**
   * Adds a join listener.
   *
   * @param listener The join listener.
   * @return The listener context.
   */
  public Listener<Member> onJoin(Consumer<Member> listener) {
    return joinListeners.add(listener);
  }

  /**
   * Leaves the member from the membership group.
   *
   * @return A completable future to be completed once the member has left.
   */
  public CompletableFuture<Void> leave() {
    return submit(GroupCommands.Leave.builder().build());
  }

  /**
   * Adds a leave listener.
   *
   * @param listener The leave listener.
   * @return The listener context.
   */
  public Listener<Member> onLeave(Consumer<Member> listener) {
    return leaveListeners.add(listener);
  }

  @Override
  protected <T> CompletableFuture<T> submit(Command<T> command) {
    return super.submit(command);
  }

}
