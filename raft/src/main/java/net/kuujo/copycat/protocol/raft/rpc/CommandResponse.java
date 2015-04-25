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
import net.kuujo.copycat.protocol.raft.RaftError;

import java.util.Objects;
import java.util.function.Function;

/**
 * Protocol command response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class CommandResponse<RESPONSE extends CommandResponse<RESPONSE>> extends AbstractResponse<RESPONSE> {
  protected Buffer result;

  public CommandResponse(ReferenceManager<RESPONSE> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the command result.
   *
   * @return The command result.
   */
  public Buffer result() {
    return result;
  }

  @Override
  public void readObject(Buffer buffer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      result = buffer.slice();
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.write(result);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public void close() {
    if (result != null)
      result.release();
    super.close();
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
    return String.format("%s[status=%s, result=%s]", getClass().getSimpleName(), status, result);
  }

  /**
   * Command response builder.
   */
  public static class Builder<BUILDER extends Builder<BUILDER, RESPONSE>, RESPONSE extends CommandResponse<RESPONSE>> extends AbstractResponse.Builder<BUILDER, RESPONSE> {

    protected Builder(Function<ReferenceManager<RESPONSE>, RESPONSE> factory) {
      super(factory);
    }

    /**
     * Sets the command response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    @SuppressWarnings("unchecked")
    public BUILDER withResult(Buffer result) {
      response.result = result;
      return (BUILDER) this;
    }

    @Override
    public RESPONSE build() {
      super.build();
      if (response.result != null)
        response.result.acquire();
      return response;
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
