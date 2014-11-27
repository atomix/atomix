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
 * Protocol sync response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface SyncResponse extends Response {

  /**
   * Returns a new sync response builder.
   *
   * @return A new sync response builder.
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
   * Returns a boolean indicating whether the sync was successful.
   *
   * @return Indicates whether the sync was successful.
   */
  boolean succeeded();

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  long logIndex();

  /**
   * Sync response builder.
   */
  static interface Builder extends Response.Builder<Builder, SyncResponse> {

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The sync response builder.
     */
    Builder withTerm(long term);

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the sync request succeeded.
     * @return The sync response builder.
     */
    Builder withSucceeded(boolean succeeded);

    /**
     * Sets the last index of the replica's log.
     *
     * @param index The last index of the replica's log.
     * @return The sync response builder.
     */
    Builder withLogIndex(long index);

  }

}
