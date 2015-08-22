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
package net.kuujo.copycat.raft.protocol.request;

import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol leave request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=264)
public class LeaveRequest extends AbstractRequest<LeaveRequest> {

  /**
   * The unique identifier for the leave request type.
   */
  public static final byte TYPE = 0x0D;

  private static final BuilderPool<Builder, LeaveRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new leave request builder.
   *
   * @return A new leave request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns an leave request builder for an existing request.
   *
   * @param request The request to build.
   * @return The leave request builder.
   */
  public static Builder builder(LeaveRequest request) {
    return POOL.acquire(request);
  }

  private Member member;

  private LeaveRequest(ReferenceManager<LeaveRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the leaving member.
   *
   * @return The leaving member.
   */
  public Member member() {
    return member;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    serializer.writeObject(member, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    member = serializer.readObject(buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LeaveRequest) {
      LeaveRequest request = (LeaveRequest) object;
      return request.member.equals(member);
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

    protected Builder(BuilderPool<Builder, LeaveRequest> pool) {
      super(pool, LeaveRequest::new);
    }

    @Override
    public void reset() {
      super.reset();
      request.member = null;
    }

    /**
     * Sets the request member.
     *
     * @param member The request member.
     * @return The request builder.
     */
    public Builder withMember(Member member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      request.member = member;
      return this;
    }

    @Override
    public LeaveRequest build() {
      super.build();

      if (request.member == null)
        throw new NullPointerException("member cannot be null");

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
