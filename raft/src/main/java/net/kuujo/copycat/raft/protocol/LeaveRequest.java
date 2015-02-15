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
 * Protocol leave request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaveRequest extends AbstractRequest {

  /**
   * Returns a new leave request builder.
   *
   * @return A new leave request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a leave request builder for an existing request.
   *
   * @param request The request to build.
   * @return The leave request builder.
   */
  public static Builder builder(LeaveRequest request) {
    return new Builder(request);
  }

  private String member;

  /**
   * Returns the member's address.
   *
   * @return The member's address.
   */
  public String member() {
    return member;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LeaveRequest) {
      LeaveRequest request = (LeaveRequest) object;
      return request.id.equals(id)
        && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), member);
  }

  /**
   * Leave request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, LeaveRequest> {
    protected Builder() {
      this(new LeaveRequest());
    }

    protected Builder(LeaveRequest request) {
      super(request);
    }

    /**
     * Sets the request member.
     *
     * @param member The request member.
     * @return The leave request builder.
     */
    public Builder withMember(String member) {
      request.member = Assert.isNotNull(member, "member");
      return this;
    }

    @Override
    public LeaveRequest build() {
      super.build();
      Assert.isNotNull(request.member, "member");
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
