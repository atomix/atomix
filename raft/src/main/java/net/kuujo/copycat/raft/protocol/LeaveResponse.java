/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.util.internal.Assert;

import java.util.Objects;

/**
 * Protocol leave response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaveResponse extends AbstractResponse {

  /**
   * Returns a new leave response builder.
   *
   * @return A new leave response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a leave response builder for an existing response.
   *
   * @param response The response to build.
   * @return The leave response builder.
   */
  public static Builder builder(LeaveResponse response) {
    return new Builder(response);
  }

  private long term;

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, status, term);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LeaveResponse) {
      LeaveResponse response = (LeaveResponse) object;
      return response.uri.equals(uri)
        && response.status == status
        && response.term == term;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d]", getClass().getSimpleName(), term);
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, LeaveResponse> {
    protected Builder() {
      this(new LeaveResponse());
    }

    protected Builder(LeaveResponse response) {
      super(response);
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The leave response builder.
     */
    public Builder withTerm(long term) {
      response.term = Assert.arg(term, term > 0, "term must be greater than zero");
      return this;
    }

    @Override
    public LeaveResponse build() {
      super.build();
      Assert.arg(response.term, response.term > 0, "term must be greater than zero");
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
