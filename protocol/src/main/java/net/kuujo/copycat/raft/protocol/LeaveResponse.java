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
import net.kuujo.copycat.raft.RaftError;

import java.util.Objects;

/**
 * Protocol leave response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=257)
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

  private boolean succeeded;

  public LeaveResponse(ReferenceManager<LeaveResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.LEAVE;
  }

  /**
   * Returns a boolean indicating whether the leave was successful.
   *
   * @return Indicates whether the leave was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      succeeded = buffer.readBoolean();
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeBoolean(succeeded);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(succeeded);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LeaveResponse) {
      LeaveResponse response = (LeaveResponse) object;
      return response.status == status
        && response.succeeded == succeeded;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, succeeded=%b]", getClass().getSimpleName(), status, succeeded);
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, LeaveResponse> {

    private Builder(BuilderPool<Builder, LeaveResponse> pool) {
      super(pool, LeaveResponse::new);
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the leave request succeeded.
     * @return The leave response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      response.succeeded = succeeded;
      return this;
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
