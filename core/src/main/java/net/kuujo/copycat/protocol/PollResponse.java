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
 * Protocol poll response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PollResponse extends Response {

  /**
   * Returns a new poll response builder.
   *
   * @return A new poll response builder.
   */
  static Builder builder() {
    return null;
  }

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  long term();

  /**
   * Returns a boolean indicating whether the vote was granted.
   *
   * @return Indicates whether the vote was granted.
   */
  boolean voted();

  /**
   * Poll response builder.
   */
  static interface Builder extends Response.Builder<Builder, PollResponse> {

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The poll response builder.
     */
    Builder withTerm(long term);

    /**
     * Sets whether the vote was granted.
     *
     * @param granted Whether the vote was granted.
     * @return The poll response builder.
     */
    Builder withVoted(boolean granted);

  }

}
