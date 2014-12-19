/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

import java.util.Objects;

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
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a sync response builder for an existing response.
   *
   * @param response The response to build.
   * @return The sync response builder.
   */
  public static Builder builder(SyncResponse response) {
    return new Builder(response);
  }

  private Object result;

  /**
   * Returns the sync result.
   *
   * @return The sync result.
   */
  @SuppressWarnings("unchecked")
  public <T> T result() {
    return (T) result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member, status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncResponse) {
      SyncResponse response = (SyncResponse) object;
      return response.id.equals(id)
        && response.member.equals(member)
        && response.status == status
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, status=%s, result=%s]", getClass().getSimpleName(), id, status, result);
  }

  /**
   * Sync response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, SyncResponse> {
    private Builder() {
      this(new SyncResponse());
    }

    private Builder(SyncResponse response) {
      super(response);
    }

    /**
     * Sets the sync response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    public Builder withResult(Object result) {
      response.result = result;
      return this;
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
