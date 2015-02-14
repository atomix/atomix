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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.raft.RaftMemberInfo;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Collection;
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

  private Collection<RaftMemberInfo> members;

  /**
   * Returns the responding node's membership.
   *
   * @return The responding node's membership.
   */
  public Collection<RaftMemberInfo> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncResponse) {
      SyncResponse response = (SyncResponse) object;
      return response.status == status
        && response.uri.equals(uri)
        && response.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), members);
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
     * Sets the response membership.
     *
     * @param members The request membership.
     * @return The sync response builder.
     */
    public Builder withMembers(Collection<RaftMemberInfo> members) {
      response.members = Assert.isNotNull(members, "members");
      return this;
    }

    @Override
    public SyncResponse build() {
      super.build();
      Assert.isNotNull(response.members, "members");
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
