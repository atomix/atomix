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

import net.kuujo.copycat.internal.util.Assert;

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

  private Membership membership;

  /**
   * Returns the responding node's membership.
   *
   * @return The responding node's membership.
   */
  public Membership membership() {
    return membership;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member, membership);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncResponse) {
      SyncResponse response = (SyncResponse) object;
      return response.id.equals(id)
        && response.status == status
        && response.member.equals(member)
        && response.membership.equals(membership);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, status=%s]", getClass().getSimpleName(), status, id);
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
     * @param membership The request membership.
     * @return The sync response builder.
     */
    public Builder withMembership(Membership membership) {
      response.membership = Assert.isNotNull(membership, "membership");
      return this;
    }

    @Override
    public SyncResponse build() {
      super.build();
      Assert.isNotNull(response.membership, "membership");
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
