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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol vote request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=274)
public class VoteRequest extends AbstractRequest<VoteRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new vote request builder.
   *
   * @return A new vote request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a vote request builder for an existing request.
   *
   * @param request The request to build.
   * @return The vote request builder.
   */
  public static Builder builder(VoteRequest request) {
    return builder.get().reset(request);
  }

  private long term;
  private int candidate;
  private long logIndex;
  private long logTerm;

  public VoteRequest(ReferenceManager<VoteRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.VOTE;
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
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    term = buffer.readLong();
    candidate = buffer.readInt();
    logIndex = buffer.readLong();
    logTerm = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeLong(term)
      .writeInt(candidate)
      .writeLong(logIndex)
      .writeLong(logTerm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, candidate, logIndex, logTerm);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof VoteRequest) {
      VoteRequest request = (VoteRequest) object;
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
   * Vote request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, VoteRequest> {

    private Builder() {
      super(VoteRequest::new);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The vote request builder.
     */
    public Builder withTerm(long term) {
      if (term <= 0)
        throw new IllegalArgumentException("term must be positive");
      request.term = term;
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param candidate The request candidate.
     * @return The vote request builder.
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
     * @return The vote request builder.
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
     * @return The vote request builder.
     */
    public Builder withLogTerm(long term) {
      if (term < 0)
        throw new NullPointerException("log term must be positive");
      request.logTerm = term;
      return this;
    }

    @Override
    public VoteRequest build() {
      super.build();
      if (request.term <= 0)
        throw new IllegalArgumentException("term must be positive");
      if (request.candidate <= 0)
        throw new IllegalArgumentException("candidate cannot be negative");
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
