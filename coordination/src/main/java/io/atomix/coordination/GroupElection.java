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
package io.atomix.coordination;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Group election.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupElection {
  private final DistributedGroup group;
  private final Listeners<Long> termListeners = new Listeners<>();
  private final Listeners<GroupMember> electionListeners = new Listeners<>();
  private final Map<String, Set<Consumer<Long>>> memberElectionListeners = new ConcurrentHashMap<>();
  volatile String leader;
  private volatile long term;

  protected GroupElection(DistributedGroup group) {
    this.group = Assert.notNull(group, "group");
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
    return leader != null ? group.member(leader) : null;
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
   * Registers an election listener callback for a specific member.
   */
  protected synchronized Listener<Long> onElection(String memberId, Consumer<Long> callback) {
    Set<Consumer<Long>> listeners = memberElectionListeners.computeIfAbsent(memberId, m -> new CopyOnWriteArraySet<>());
    listeners.add(callback);
    Listener<Long> listener = new Listener<Long>() {
      @Override
      public void accept(Long term) {
        callback.accept(term);
      }
      @Override
      public void close() {
        listeners.remove(this);
        synchronized (GroupElection.this) {
          if (listeners.isEmpty()) {
            memberElectionListeners.remove(memberId);
          }
        }
      }
    };

    if (leader != null && leader.equals(memberId)) {
      listener.accept(term);
    }
    return listener;
  }

  /**
   * Handles a term change event received from the cluster.
   */
  void onTermEvent(long term) {
    this.term = term;
    termListeners.accept(term);
  }

  /**
   * Handles an elect event received from the cluster.
   */
  void onElectEvent(String leader) {
    this.leader = leader;
    GroupMember member = group.member(leader);
    if (member != null) {
      electionListeners.accept(member);
      Set<Consumer<Long>> listeners = memberElectionListeners.get(member.id());
      if (listeners != null) {
        listeners.forEach(c -> c.accept(term));
      }
    }
  }

  /**
   * Handles a resign event received from the cluster.
   */
  void onResignEvent(String leader) {
    if (this.leader != null && this.leader.equals(leader)) {
      this.leader = null;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
