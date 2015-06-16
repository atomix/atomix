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
package net.kuujo.copycat.raft.rpc;

import net.kuujo.copycat.io.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol keep alive request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveRequest extends SessionRequest<KeepAliveRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new keep alive request builder.
   *
   * @return A new keep alive request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a keep alive request builder for an existing request.
   *
   * @param request The request to build.
   * @return The keep alive request builder.
   */
  public static Builder builder(KeepAliveRequest request) {
    return builder.get().reset(request);
  }

  public KeepAliveRequest(ReferenceManager<KeepAliveRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.KEEP_ALIVE;
  }

  @Override
  public int hashCode() {
    return Objects.hash(session);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveRequest) {
      KeepAliveRequest request = (KeepAliveRequest) object;
      return request.session == session;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d]", getClass().getSimpleName(), session);
  }

  /**
   * Keep alive request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, KeepAliveRequest> {

    private Builder() {
      super(KeepAliveRequest::new);
    }

    @Override
    public KeepAliveRequest build() {
      super.build();
      if (request.session <= 0)
        throw new IllegalArgumentException("session must be positive");
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
