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
import net.kuujo.copycat.protocol.Persistence;

import java.util.Objects;

/**
 * Protocol command request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitRequest extends AbstractRequest<SubmitRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a submit request builder for an existing request.
   *
   * @param request The request to build.
   * @return The submit request builder.
   */
  public static Builder builder(SubmitRequest request) {
    return builder.get().reset(request);
  }

  private Buffer entry;
  private Persistence persistence = Persistence.DEFAULT;
  private Consistency consistency = Consistency.DEFAULT;

  public SubmitRequest(ReferenceManager<SubmitRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.SUBMIT;
  }

  /**
   * Returns the command entry.
   *
   * @return The command entry.
   */
  public Buffer entry() {
    return entry;
  }

  /**
   * Returns the command persistence level.
   *
   * @return The command persistence level.
   */
  public Persistence persistence() {
    return persistence;
  }

  /**
   * Returns the command consistency level.
   *
   * @return The command consistency level.
   */
  public Consistency consistency() {
    return consistency;
  }

  @Override
  public void readObject(Buffer buffer) {
    persistence = Persistence.values()[buffer.readByte()];
    consistency = Consistency.values()[buffer.readByte()];
    int entrySize = buffer.readInt();
    entry = buffer.slice(entrySize);
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeByte(persistence.ordinal());
    buffer.writeByte(consistency.ordinal());
    buffer.writeInt((int) entry.limit()).write(entry);
  }

  @Override
  public void close() {
    if (entry != null)
      entry.release();
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(entry, persistence, consistency);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof SubmitRequest && ((SubmitRequest) object).entry.equals(entry);
  }

  @Override
  public String toString() {
    return String.format("%s[entry=%s, persistence=%s, consistency=%s]", getClass().getSimpleName(), entry, persistence, consistency);
  }

  /**
   * Write request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, SubmitRequest> {

    protected Builder() {
      super(SubmitRequest::new);
    }

    /**
     * Sets the request entry.
     *
     * @param entry The request entry.
     * @return The request builder.
     */
    @SuppressWarnings("unchecked")
    public Builder withEntry(Buffer entry) {
      if (entry == null)
        throw new NullPointerException("entry cannot be null");
      request.entry = entry;
      return this;
    }

    /**
     * Sets the request persistence level.
     *
     * @param persistence The request persistence level.
     * @return The request builder.
     */
    public Builder withPersistence(Persistence persistence) {
      if (persistence == null)
        throw new NullPointerException("persistence cannot be null");
      request.persistence = persistence;
      return this;
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

    @Override
    public SubmitRequest build() {
      super.build();
      if (request.entry != null)
        request.entry.acquire();
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
