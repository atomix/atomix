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
public class PingResponse extends AbstractResponse {

  /**
   * Returns a new ping response builder.
   *
   * @return A new ping response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private long term;
  private boolean succeeded;

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
   * Ping response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, PingResponse> {
    private Builder() {
      super(new PingResponse());
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The ping response builder.
     */
    public Builder withTerm(long term) {
      response.term = term;
      return this;
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the request succeeded.
     * @return The ping response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      response.succeeded = succeeded;
      return this;
    }

  }

}
