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

import net.kuujo.copycat.internal.util.Assert;

import java.util.Objects;

/**
 * Protocol append response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AppendResponse extends AbstractResponse {
  public static final int TYPE = -6;

  /**
   * Returns a new append response builder.
   *
   * @return A new append response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private long term;
  private boolean succeeded;
  private Long logIndex;

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the append was successful.
   *
   * @return Indicates whether the append was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  public Long logIndex() {
    return logIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member, term, succeeded, logIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendResponse) {
      AppendResponse response = (AppendResponse) object;
      return response.id.equals(id)
        && response.status == status
        && response.member.equals(member)
        && response.term == term
        && response.succeeded == succeeded
        && response.logIndex.equals(logIndex);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, status=%s, term=%d, succeeded=%b, logIndex=%d]", getClass().getSimpleName(), status, id, term, succeeded, logIndex);
  }

  /**
   * Append response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, AppendResponse> {
    private Builder() {
      super(new AppendResponse());
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The append response builder.
     */
    public Builder withTerm(long term) {
      response.term = term;
      return this;
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the append request succeeded.
     * @return The append response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      response.succeeded = succeeded;
      return this;
    }

    /**
     * Sets the last index of the replica's log.
     *
     * @param index The last index of the replica's log.
     * @return The append response builder.
     */
    public Builder withLogIndex(Long index) {
      response.logIndex = Assert.index(index, index == null || index > 0, "log index must be greater than zero");
      return this;
    }

    @Override
    public AppendResponse build() {
      super.build();
      Assert.arg(response.term, response.term > 0, "term must be greater than zero");
      Assert.index(response.logIndex, response.logIndex == null || response.logIndex > 0, "log index must be greater than zero");
      return response;
    }

    @Override
    public int hashCode() {
      return Objects.hash(response);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).response.equals(response);
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }

  }

}
