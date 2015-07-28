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

import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol publish response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=269)
public class PublishResponse extends AbstractResponse<PublishResponse> {
  private static final BuilderPool<Builder, PublishResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new publish response builder.
   *
   * @return A new publish response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a publish response builder for an existing response.
   *
   * @param response The response to build.
   * @return The publish response builder.
   */
  public static Builder builder(PublishResponse response) {
    return POOL.acquire(response);
  }

  public PublishResponse(ReferenceManager<PublishResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.PUBLISH;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.ERROR) {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishResponse) {
      PublishResponse response = (PublishResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Publish response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, PublishResponse> {

    private Builder(BuilderPool<Builder, PublishResponse> pool) {
      super(pool, PublishResponse::new);
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
