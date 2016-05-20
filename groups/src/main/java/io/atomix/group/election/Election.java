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
package io.atomix.group.election;

import io.atomix.catalyst.concurrent.Listener;

import java.util.function.Consumer;

/**
 * Election context for {@link io.atomix.group.DistributedGroup}.
 * <p>
 * Elections are performed automatically within each {@link io.atomix.group.DistributedGroup}. As members
 * are added to or removed from the group, the group will automatically elect group leaders. Each unique
 * leader election is represented by a {@link Term}. The {@link Term#term()} is guaranteed to be unique
 * and monotonically increasing throughout the lifetime of a group. When a leader disconnects from the
 * cluster or leaves the group, the term will be increased and a new leader elected. Leaders are elected
 * using a semi-random algorithm. Each instance of a group is guaranteed to see the same leader and term
 * at the same <em>logical</em> time (not real time).
 * <p>
 * To access the current group term, use the {@link #term()}} getter.
 * <pre>
 *   {@code
 *   Term currentTerm = group.election().term();
 *   }
 * </pre>
 * Clients can listen for changes in group leadership by registering an election listener via
 * {@link #onElection(Consumer)}.
 * <pre>
 *   {@code
 *   group.election().onElection(term -> {
 *     GroupMember leader = term.leader();
 *   });
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Election {

  /**
   * Returns the current group term.
   * <p>
   * If the group is undergoing a new election, the returned {@link Term} can potentially have a
   * null {@link Term#leader()}. To ensure the term has a leader, either check the value or use
   * an {@link #onElection(Consumer) election listener} to listen for new leaders.
   *
   * @return The current group term.
   */
  Term term();

  /**
   * Registers an election listener callback.
   * <p>
   * The provided callback will be called each time the election is changed to a new term and a leader
   * is elected. The {@link Term} provided to the election callback is guaranteed to have a monotonically
   * increasing {@link Term#term() term number} and a non-null {@link Term#leader()}.
   * <pre>
   *   {@code
   *   group.election().onElection(term -> {
   *     GroupMember leader = term.leader();
   *   });
   *   }
   * </pre>
   * If a leader was already elected for the current term when the callback is registered, the callback will
   * be immediately called <em>before the completion of the registration</em>. Aside from this initial notification,
   * all election listeners are guaranteed to see elections at the same logical time and in the same order on all
   * nodes.
   *
   * @param callback The callback to be called when a new leader is elected.
   * @return The election listener.
   */
  Listener<Term> onElection(Consumer<Term> callback);

}
