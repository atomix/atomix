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
 * Protocol join response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JoinResponse extends AbstractResponse {

  /**
   * Returns a new join response builder.
   *
   * @return A new join response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a join response builder for an existing response.
   *
   * @param response The response to build.
   * @return The join response builder.
   */
  public static Builder builder(JoinResponse response) {
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
    return Objects.hash(id, status, term);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof JoinResponse) {
      JoinResponse response = (JoinResponse) object;
      return response.id.equals(id)
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
  public static class Builder extends AbstractResponse.Builder<Builder, JoinResponse> {
    protected Builder() {
      this(new JoinResponse());
    }

    protected Builder(JoinResponse response) {
      super(response);
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The join response builder.
     */
    public Builder withTerm(long term) {
      response.term = Assert.arg(term, term > 0, "term must be greater than zero");
      return this;
    }

    @Override
    public JoinResponse build() {
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
