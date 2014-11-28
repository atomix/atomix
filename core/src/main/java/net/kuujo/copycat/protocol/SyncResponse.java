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
public class SyncResponse extends AbstractResponse {

  /**
   * Returns a new sync response builder.
   *
   * @return A new sync response builder.
   */
  static Builder builder() {
    return new Builder();
  }

  private long term;
  private boolean succeeded;
  private long logIndex;

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the sync was successful.
   *
   * @return Indicates whether the sync was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Sync response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, SyncResponse> {
    private Builder() {
      super(new SyncResponse());
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The sync response builder.
     */
    public Builder withTerm(long term) {
      response.term = term;
      return this;
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the sync request succeeded.
     * @return The sync response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      response.succeeded = succeeded;
      return this;
    }

    /**
     * Sets the last index of the replica's log.
     *
     * @param index The last index of the replica's log.
     * @return The sync response builder.
     */
    public Builder withLogIndex(long index) {
      response.logIndex = index;
      return this;
    }

  }

}
