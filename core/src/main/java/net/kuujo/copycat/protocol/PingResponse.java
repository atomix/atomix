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
 * Protocol ping response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PingResponse extends Response {

  /**
   * Returns a new ping response builder.
   *
   * @return A new ping response builder.
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
   * Ping response builder.
   */
  static interface Builder extends Response.Builder<Builder, PingResponse> {

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The ping response builder.
     */
    Builder withTerm(long term);

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the request succeeded.
     * @return The ping response builder.
     */
    Builder withSucceeded(boolean succeeded);

  }

}
