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

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol publish request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=268)
public class PublishRequest extends SessionRequest<PublishRequest> {

  /**
   * The unique identifier for the publish request type.
   */
  public static final byte TYPE = 0x05;

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

  private long eventSequence;
  private Object message;

  public PublishRequest(ReferenceManager<PublishRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the event sequence number.
   *
   * @return The event sequence number.
   */
  public long eventSequence() {
    return eventSequence;
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
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    eventSequence = buffer.readLong();
    message = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(eventSequence);
    serializer.writeObject(message, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, eventSequence, message);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishRequest) {
      PublishRequest request = (PublishRequest) object;
      return request.session == session
        && request.eventSequence == eventSequence
        && request.message.equals(message);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, eventSequence=%d, message=%s]", getClass().getSimpleName(), session, eventSequence, message);
  }

  /**
   * Publish request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, PublishRequest> {

    protected Builder(BuilderPool<Builder, PublishRequest> pool) {
      super(pool, PublishRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.eventSequence = 0;
      request.message = null;
    }

    /**
     * Sets the event sequence number.
     *
     * @param eventSequence The event sequence number.
     * @return The request builder.
     */
    public Builder withEventSequence(long eventSequence) {
      if (eventSequence <= 0)
        throw new IllegalArgumentException("eventSequence cannot be less than 1");
      request.eventSequence = eventSequence;
      return this;
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
      if (request.eventSequence <= 0)
        throw new IllegalArgumentException("eventSequence cannot be less than 1");
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
