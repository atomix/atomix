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
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol leave response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=265)
public class LeaveResponse extends AbstractResponse<LeaveResponse> {
  private static final BuilderPool<Builder, LeaveResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new leave response builder.
   *
   * @return A new leave response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns an leave response builder for an existing response.
   *
   * @param response The response to build.
   * @return The leave response builder.
   */
  public static Builder builder(LeaveResponse response) {
    return POOL.acquire(response);
  }

  public LeaveResponse(ReferenceManager<LeaveResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.LEAVE;
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
    if (status == Status.OK) {
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LeaveResponse) {
      LeaveResponse response = (LeaveResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, LeaveResponse> {

    protected Builder(BuilderPool<Builder, LeaveResponse> pool) {
      super(pool, LeaveResponse::new);
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
