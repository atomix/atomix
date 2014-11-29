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
public class PollResponse extends AbstractResponse {

  /**
   * Returns a new poll response builder.
   *
   * @return A new poll response builder.
   */
  public static Builder builder() {
    return null;
  }

  private long term;
  private boolean voted;

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the vote was granted.
   *
   * @return Indicates whether the vote was granted.
   */
  public boolean voted() {
    return voted;
  }

  /**
   * Poll response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, PollResponse> {
    private Builder() {
      super(new PollResponse());
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The poll response builder.
     */
    public Builder withTerm(long term) {
      response.term = term;
      return this;
    }

    /**
     * Sets whether the vote was granted.
     *
     * @param voted Whether the vote was granted.
     * @return The poll response builder.
     */
    public Builder withVoted(boolean voted) {
      response.voted = voted;
      return this;
    }

  }

}
