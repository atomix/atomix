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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.RaftError;

import java.util.Objects;

/**
 * Protocol sync response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncResponse extends AbstractResponse<SyncResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new sync response builder.
   *
   * @return A new sync response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a sync response builder for an existing response.
   *
   * @param response The response to build.
   * @return The sync response builder.
   */
  public static Builder builder(SyncResponse response) {
    return builder.get().reset(response);
  }

  public SyncResponse(ReferenceManager<SyncResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.SYNC;
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    status = Response.Status.forId(buffer.readByte());
    if (status == Response.Status.OK) {
      error = null;
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.ERROR) {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, error);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncResponse) {
      SyncResponse response = (SyncResponse) object;
      return response.status == status;
    }
    return false;
  }

  /**
   * Sync response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, SyncResponse> {

    private Builder() {
      super(SyncResponse::new);
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
