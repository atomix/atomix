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
 * Protocol commit response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommitResponse extends AbstractResponse {
  public static final int TYPE = -12;

  /**
   * Returns a new commit response builder.
   *
   * @return A new commit response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private Object result;

  /**
   * Returns the commit result.
   *
   * @return The commit result.
   */
  @SuppressWarnings("unchecked")
  public <T> T result() {
    return (T) result;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, result=%s]", getClass().getSimpleName(), id, result);
  }

  /**
   * Commit response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, CommitResponse> {
    private Builder() {
      super(new CommitResponse());
    }

    /**
     * Sets the commit response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    public Builder withResult(Object result) {
      response.result = result;
      return this;
    }
  }

}
