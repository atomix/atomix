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
package net.kuujo.copycat.protocol;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.RaftError;

import java.util.Objects;

/**
 * Protocol command response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=259)
public class CommandResponse extends ClientResponse<CommandResponse> {
  private static final BuilderPool<Builder, CommandResponse> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new submit response builder.
   *
   * @return A new submit response builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a submit response builder for an existing request.
   *
   * @param request The response to build.
   * @return The submit response builder.
   */
  public static Builder builder(CommandResponse request) {
    return POOL.acquire(request);
  }

  private Object result;

  public CommandResponse(ReferenceManager<CommandResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.COMMAND;
  }

  /**
   * Returns the command result.
   *
   * @return The command result.
   */
  public Object result() {
    return result;
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      version = buffer.readLong();
      result = alleycat.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(version);
      alleycat.writeObject(result, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandResponse) {
      CommandResponse response = (CommandResponse) object;
      return response.status == status
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, version=%d, result=%s]", getClass().getSimpleName(), status, version, result);
  }

  /**
   * Command response builder.
   */
  public static class Builder extends ClientResponse.Builder<Builder, CommandResponse> {

    private Builder(BuilderPool<Builder, CommandResponse> pool) {
      super(pool, CommandResponse::new);
    }

    @Override
    protected void reset() {
      super.reset();
      response.result = null;
    }

    /**
     * Sets the command response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withResult(Object result) {
      response.result = result;
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
