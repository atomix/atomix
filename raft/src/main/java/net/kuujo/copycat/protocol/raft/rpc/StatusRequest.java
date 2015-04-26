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
package net.kuujo.copycat.protocol.raft.rpc;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol status request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StatusRequest extends AbstractRequest<StatusRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new status request builder.
   *
   * @return A new status request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a status request builder for an existing request.
   *
   * @param request The request to build.
   * @return The status request builder.
   */
  public static Builder builder(StatusRequest request) {
    return builder.get().reset(request);
  }

  private int id;

  public StatusRequest(ReferenceManager<StatusRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.STATUS;
  }

  /**
   * Returns the requesting node's ID.
   *
   * @return The requesting node's ID.
   */
  public int id() {
    return id;
  }

  @Override
  public void readObject(Buffer buffer) {
    id = buffer.readInt();
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeInt(id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof StatusRequest) {
      StatusRequest request = (StatusRequest) object;
      return request.id == id;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), id);
  }

  /**
   * Status request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, StatusRequest> {

    private Builder() {
      super(StatusRequest::new);
    }

    /**
     * Sets the requesting node's ID.
     *
     * @param id The requesting node's ID.
     * @return The status request builder.
     */
    public Builder withId(int id) {
      if (id <= 0)
        throw new IllegalArgumentException("id must be positive");
      request.id = id;
      return this;
    }

    @Override
    public StatusRequest build() {
      super.build();
      if (request.id <= 0)
        throw new IllegalArgumentException("id must be positive");
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
