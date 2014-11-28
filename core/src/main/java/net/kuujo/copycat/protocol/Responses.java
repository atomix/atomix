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
 * Response types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
enum Responses {

  /**
   * Configure response.
   */
  CONFIGURE(1, ConfigureResponse.class),

  /**
   * Ping response.
   */
  PING(3, PingResponse.class),

  /**
   * Poll response.
   */
  POLL(5, PollResponse.class),

  /**
   * Sync response.
   */
  SYNC(7, SyncResponse.class),

  /**
   * Commit response.
   */
  COMMIT(9, CommitResponse.class);

  private final int id;
  private final Class<? extends Response> type;

  private Responses(int id, Class<? extends Response> type) {
    this.id = id;
    this.type = type;
  }

  /**
   * Returns the response ID.
   *
   * @return The response ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the response type.
   *
   * @return The response type.
   */
  public Class<? extends Response> type() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("Response[%s]", name());
  }

}
