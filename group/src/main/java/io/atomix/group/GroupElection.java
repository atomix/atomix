/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Manages an election among {@link DistributedGroup} members.
 * <p>
 * Within each group, a leader is elected automatically by the group's replicated state machine.
 * Group elections are fair, meaning the first member to join the group will always be elected the
 * leader for the first term. Elections provide a unique, monotonically increasing token called
 * a {@link #term() term} which can be used to resolve leader conflicts across group members. At
 * most one leader is guaranteed to exist for any term, and the leader for a given term is guaranteed
 * to be the same for all members of the group.
 * <p>
 * The group-wide election can be accessed on the {@link DistributedGroup} object via
 * {@link DistributedGroup#election()}. Because they're low overhead - action only needs to be taken
 * when a member joins or leaves the group - group members always participate in an election.
 * <p>
 * To listen for a group member to be elected leader, register a {@link #onElection(Consumer) election listener}.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("election-group").get();
 *
 *   group.onElection(term -> {
 *     term.leader().connection().send("hi!");
 *   });
 *   }
 * </pre>
 * The election listener callback will be called with the {@link GroupMember} that was elected leader
 * once complete. Similarly, you can access the current leader via the {@link GroupTerm#leader()} getter. The
 * current leader is guaranteed to be the leader for the current {@link #term()}. However, the leader
 * may be {@code null} if no leader has been elected for the current term.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupElection {
  private final DistributedGroup group;
  private final Listeners<GroupTerm> electionListeners = new Listeners<>();
  private volatile GroupTerm term;

  protected GroupElection(DistributedGroup group) {
    this.group = Assert.notNull(group, "group");
  }

  /**
   * Returns the current election term.
   *
   * @return The current election term.
   */
  public GroupTerm term() {
    return term;
  }

  /**
   * Registers a callback to be called when a member of the group is elected leader.
   * <p>
   * The provided callback will be called when notification of a leader change is received by the resource.
   * The leader will be assigned only after the {@link #term()} has been incremented. When the callback is
   * called, the leader is guaranteed to be associated with the current term, and no other leader will ever
   * be associated with the same term (assuming the cluster is persistent).
   * <p>
   * The returned {@link Listener} can be used to unregister the term listener via {@link Listener#close()}.
   * <pre>
   *   {@code
   *   Listener<GroupMember> listener = group.election().onElection(leader -> {
   *     ...
   *   });
   *
   *   // Unregister the listener
   *   listener.close();
   *   }
   * </pre>
   *
   * @param callback The callback to call when a member of the group is elected leader.
   * @return The leader election listener.
   */
  public synchronized Listener<GroupTerm> onElection(Consumer<GroupTerm> callback) {
    Listener<GroupTerm> listener = electionListeners.add(callback);
    if (term != null) {
      listener.accept(term);
    }
    return listener;
  }

  /**
   * Called when a member joins the election.
   */
  synchronized void onJoin(GroupMember member) {
    if (term == null || term.term() != term.leader().index()) {
      elect();
    }
  }

  /**
   * Called when a member leaves the election.
   */
  synchronized void onLeave(GroupMember member) {
    if (term != null && term.leader().equals(member)) {
      elect();
    }
  }

  /**
   * Elects a new leader.
   */
  private synchronized void elect() {
    term = null;
    Collection<GroupMember> members = group.members();
    if (!members.isEmpty()) {
      GroupMember leader = group.members().stream().sorted((m1, m2) -> (int) (m1.index() - m2.index())).findFirst().get();
      term = new GroupTerm(leader.index(), leader);
      electionListeners.accept(term);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
