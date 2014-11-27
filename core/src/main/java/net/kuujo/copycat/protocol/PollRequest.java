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
 * Protocol poll request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PollRequest extends Request {

  /**
   * Returns a new poll request builder.
   *
   * @return A new poll request builder.
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
   * Returns the candidate's address.
   *
   * @return The candidate's address.
   */
  String candidate();

  /**
   * Returns the candidate's last log index.
   *
   * @return The candidate's last log index.
   */
  long logIndex();

  /**
   * Returns the candidate's last log term.
   *
   * @return The candidate's last log term.
   */
  long logTerm();

  /**
   * Poll request builder.
   */
  static interface Builder extends Request.Builder<Builder, PollRequest> {

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The poll request builder.
     */
    Builder withTerm(long term);

    /**
     * Sets the request leader.
     *
     * @param candidate The request candidate.
     * @return The poll request builder.
     */
    Builder withCandidate(String candidate);

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The poll request builder.
     */
    Builder withLogIndex(long index);

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The poll request builder.
     */
    Builder withLogTerm(long term);

  }

}
