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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.BuilderPool;

import java.util.Objects;

/**
 * Protocol publish request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=268)
public class PublishRequest extends SessionRequest<PublishRequest> {
  private static final BuilderPool<Builder, PublishRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new publish request builder.
   *
   * @return A new publish request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a publish request builder for an existing request.
   *
   * @param request The request to build.
   * @return The publish request builder.
   */
  public static Builder builder(PublishRequest request) {
    return POOL.acquire(request);
  }

  private Object message;

  public PublishRequest(ReferenceManager<PublishRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.PUBLISH;
  }

  /**
   * Returns the request message.
   *
   * @return The request message.
   */
  public Object message() {
    return message;
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    message = alleycat.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    alleycat.writeObject(message, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishRequest) {
      PublishRequest request = (PublishRequest) object;
      return request.message.equals(message);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[message=%s]", getClass().getSimpleName(), message);
  }

  /**
   * Publish request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, PublishRequest> {

    private Builder(BuilderPool<Builder, PublishRequest> pool) {
      super(pool, PublishRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.message = null;
    }

    /**
     * Sets the request message.
     *
     * @param message The request message.
     * @return The publish request builder.
     */
    public Builder withMessage(Object message) {
      if (message == null)
        throw new NullPointerException("message cannot be null");
      request.message = message;
      return this;
    }

    @Override
    public PublishRequest build() {
      super.build();
      if (request.message == null)
        throw new NullPointerException("message cannot be null");
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
