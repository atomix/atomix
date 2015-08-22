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
package net.kuujo.copycat.raft.protocol.request;

import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol poll request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=266)
public class PollRequest extends AbstractRequest<PollRequest> {

  /**
   * The unique identifier for the poll request type.
   */
  public static final byte TYPE = 0x0F;

  private static final BuilderPool<Builder, PollRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new poll request builder.
   *
   * @return A new poll request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a poll request builder for an existing request.
   *
   * @param request The request to build.
   * @return The poll request builder.
   */
  public static Builder builder(PollRequest request) {
    return POOL.acquire(request);
  }

  private long term;
  private int candidate;
  private long logIndex;
  private long logTerm;

  public PollRequest(ReferenceManager<PollRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the candidate's address.
   *
   * @return The candidate's address.
   */
  public int candidate() {
    return candidate;
  }

  /**
   * Returns the candidate's last log index.
   *
   * @return The candidate's last log index.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Returns the candidate's last log term.
   *
   * @return The candidate's last log term.
   */
  public long logTerm() {
    return logTerm;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    term = buffer.readLong();
    candidate = buffer.readInt();
    logIndex = buffer.readLong();
    logTerm = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(term)
      .writeInt(candidate)
      .writeLong(logIndex)
      .writeLong(logTerm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, candidate, logIndex, logTerm);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PollRequest) {
      PollRequest request = (PollRequest) object;
      return request.term == term
        && request.candidate == candidate
        && request.logIndex == logIndex
        && request.logTerm == logTerm;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, candidate=%s, logIndex=%d, logTerm=%d]", getClass().getSimpleName(), term, candidate, logIndex, logTerm);
  }

  /**
   * Poll request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, PollRequest> {

    protected Builder(BuilderPool<Builder, PollRequest> pool) {
      super(pool, PollRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.term = 0;
      request.candidate = 0;
      request.logIndex = 0;
      request.logTerm = 0;
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The poll request builder.
     */
    public Builder withTerm(long term) {
      if (term < 0)
        throw new IllegalArgumentException("term must be positive");
      request.term = term;
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param candidate The request candidate.
     * @return The poll request builder.
     */
    public Builder withCandidate(int candidate) {
      if (candidate <= 0)
        throw new IllegalArgumentException("candidate must be positive");
      request.candidate = candidate;
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The poll request builder.
     */
    public Builder withLogIndex(long index) {
      if (index < 0)
        throw new IllegalArgumentException("log index must be positive");
      request.logIndex = index;
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The poll request builder.
     */
    public Builder withLogTerm(long term) {
      if (term < 0)
        throw new IllegalArgumentException("log term must be positive");
      request.logTerm = term;
      return this;
    }

    @Override
    public PollRequest build() {
      super.build();
      if (request.term < 0)
        throw new IllegalArgumentException("term must be positive");
      if (request.candidate <= 0)
        throw new NullPointerException("candidate must be positive");
      if (request.logIndex < 0)
        throw new IllegalArgumentException("log index must be positive");
      if (request.logTerm < 0)
        throw new IllegalArgumentException("log term must be positive");
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
