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
 * Request types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
enum Requests {

  /**
   * Configure request.
   */
  CONFIGURE(0, ConfigureRequest.class),

  /**
   * Ping request.
   */
  PING(2, PingRequest.class),

  /**
   * Poll request.
   */
  POLL(4, PollRequest.class),

  /**
   * Sync request.
   */
  SYNC(6, SyncRequest.class),

  /**
   * Commit request.
   */
  COMMIT(8, CommitRequest.class);

  private final int id;
  private final Class<? extends Request> type;

  private Requests(int id, Class<? extends Request> type) {
    this.id = id;
    this.type = type;
  }

  /**
   * Returns the request ID.
   *
   * @return The request ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the request type.
   *
   * @return The request type.
   */
  public Class<? extends Request> type() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("Request[%s]", name());
  }

}
