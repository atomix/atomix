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
import net.kuujo.copycat.protocol.Consistency;

import java.util.Objects;

/**
 * Protocol read request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReadRequest extends CommandRequest<ReadRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new read request builder.
   *
   * @return A new read request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a read request builder for an existing request.
   *
   * @param request The request to build.
   * @return The read request builder.
   */
  public static Builder builder(ReadRequest request) {
    return builder.get().reset(request);
  }

  private Consistency consistency = Consistency.DEFAULT;

  public ReadRequest(ReferenceManager<ReadRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.READ;
  }

  /**
   * Returns the read consistency level.
   *
   * @return The read consistency level.
   */
  public Consistency consistency() {
    return consistency;
  }

  @Override
  public void readObject(Buffer buffer) {
    consistency = Consistency.values()[buffer.readByte()];
    super.readObject(buffer);
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeByte(consistency.ordinal());
    super.writeObject(buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entry, consistency);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReadRequest) {
      ReadRequest request = (ReadRequest) object;
      return request.entry.equals(entry)
        && request.consistency == consistency;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[key=%s, entry=%s, consistency=%s]", getClass().getSimpleName(), key != null ? key.toString() : "", entry != null ? entry.toString() : "", consistency);
  }

  /**
   * Read request builder.
   */
  public static class Builder extends CommandRequest.Builder<Builder, ReadRequest> {
    private Builder() {
      super(ReadRequest::new);
    }

    /**
     * Sets the request consistency level.
     *
     * @param consistency The request consistency level.
     * @return The request builder.
     */
    public Builder withConsistency(Consistency consistency) {
      if (consistency == null)
        throw new NullPointerException("consistency cannot be null");
      request.consistency = consistency;
      return this;
    }
  }

}
