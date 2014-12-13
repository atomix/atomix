/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.RaftProtocol;
import net.kuujo.copycat.spi.ExecutionContext;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Copycat state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CopycatStateContext extends RaftProtocol {

  /**
   * Returns the log consumer.
   *
   * @return The log consumer.
   */
  BiFunction<Long, ByteBuffer, ByteBuffer> consumer();

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  Log log();

  /**
   * Returns the context executor.
   *
   * @return The context executor.
   */
  ExecutionContext executor();

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  String getLocalMember();

  /**
   * Adds a member to the state cluster.
   *
   * @param uri The URI of the member to add.
   * @return The Copycat state context.
   */
  CopycatStateContext addMember(String uri);

  /**
   * Removes a member from the state cluster.
   *
   * @param uri The URI of the member to remove.
   * @return The Copycat state context.
   */
  CopycatStateContext removeMember(String uri);

  /**
   * Sets all members on the state cluster.
   *
   * @param members A set of members in the state cluster.
   * @return The Copycat state context.
   */
  CopycatStateContext setMembers(Set<String> members);

  /**
   * Returns a set of all members in the state cluster.
   *
   * @return A set of all members in the state cluster.
   */
  Set<String> getMembers();

  /**
   * Returns a set of remote members in the state cluster.
   *
   * @return A set of remote members in the state cluster.
   */
  Set<String> getRemoteMembers();

  /**
   * Returns the current Copycat election status.
   *
   * @return The current Copycat election status.
   */
  Election.Status getStatus();

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Copycat state context.
   */
  CopycatStateContext setLeader(String leader);

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  String getLeader();

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Copycat state context.
   */
  CopycatStateContext setTerm(long term);

  /**
   * Returns the state term.
   *
   * @return The state term.
   */
  long getTerm();

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Copycat state context.
   */
  CopycatStateContext setLastVotedFor(String candidate);

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  String getLastVotedFor();

  /**
   * Sets the state commit index.
   *
   * @param commitIndex The state commit index.
   * @return The Copycat state context.
   */
  CopycatStateContext setCommitIndex(long commitIndex);

  /**
   * Returns the state commit index.
   *
   * @return The state commit index.
   */
  long getCommitIndex();

  /**
   * Sets the state last applied index.
   *
   * @param lastApplied The state last applied index.
   * @return The Copycat state context.
   */
  CopycatStateContext setLastApplied(long lastApplied);

  /**
   * Returns the state last applied index.
   *
   * @return The state last applied inex.
   */
  long getLastApplied();

  /**
   * Sets the state election timeout.
   *
   * @param electionTimeout The state election timeout.
   * @return The Copycat state context.
   */
  CopycatStateContext setElectionTimeout(long electionTimeout);

  /**
   * Returns the state election timeout.
   *
   * @return The state election timeout.
   */
  long getElectionTimeout();

  /**
   * Sets the state heartbeat interval.
   *
   * @param heartbeatInterval The state heartbeat interval.
   * @return The Copycat state context.
   */
  CopycatStateContext setHeartbeatInterval(long heartbeatInterval);

  /**
   * Returns the state heartbeat interval.
   *
   * @return The state heartbeat interval.
   */
  long getHeartbeatInterval();

}
