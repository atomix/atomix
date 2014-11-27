/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.protocol;

/**
 * Protocol ping request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PingRequest extends Request {

  /**
   * Returns a new ping request builder.
   *
   * @return A new ping request builder.
   */
  static Builder builder() {
    return null;
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  long term();

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  String leader();

  /**
   * Returns the index of the entry in the leader's log.
   *
   * @return The index of the entry in the leader's log.
   */
  long logIndex();

  /**
   * Returns the term of the entry in the leader's log.
   *
   * @return The term of the entry in the leader's log.
   */
  long logTerm();

  /**
   * Returns the leader's commit index.
   *
   * @return The leader commit index.
   */
  long commitIndex();

  /**
   * Ping request builder.
   */
  static interface Builder extends Request.Builder<Builder, PingRequest> {

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The ping request builder.
     */
    Builder withTerm(long term);

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The ping request builder.
     */
    Builder withLeader(String leader);

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The ping request builder.
     */
    Builder withLogIndex(long index);

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The ping request builder.
     */
    Builder withLogTerm(long term);

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The ping request builder.
     */
    Builder withCommitIndex(long index);

  }

}
