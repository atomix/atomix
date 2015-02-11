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
 * Protocol promote response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PromoteResponse extends AbstractResponse {

  /**
   * Returns a new promote response builder.
   *
   * @return A new promote response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a promote response builder for an existing response.
   *
   * @param response The response to build.
   * @return The promote response builder.
   */
  public static Builder builder(PromoteResponse response) {
    return new Builder(response);
  }

  private long term;
  private boolean succeeded;

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns whether the promotion succeeded.
   *
   * @return Indicates whether the promotion succeeded.
   */
  public boolean succeeded() {
    return succeeded;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, status, term);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PromoteResponse) {
      PromoteResponse response = (PromoteResponse) object;
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
   * Poll response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, PromoteResponse> {
    protected Builder() {
      this(new PromoteResponse());
    }

    protected Builder(PromoteResponse response) {
      super(response);
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The promote response builder.
     */
    public Builder withTerm(long term) {
      response.term = Assert.arg(term, term > 0, "term must be greater than zero");
      return this;
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Indicates whether the promotion succeeded.
     * @return The promote response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      response.succeeded = succeeded;
      return this;
    }

    @Override
    public PromoteResponse build() {
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
