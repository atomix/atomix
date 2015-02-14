/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.util.internal.Assert;

import java.util.Objects;

/**
 * Protocol vote request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VoteRequest extends AbstractRequest {

  /**
   * Returns a new vote request builder.
   *
   * @return A new vote request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a vote request builder for an existing request.
   *
   * @param request The request to build.
   * @return The vote request builder.
   */
  public static Builder builder(VoteRequest request) {
    return new Builder(request);
  }

  private long term;
  private String candidate;
  private Long logIndex;
  private Long logTerm;

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
  public String candidate() {
    return candidate;
  }

  /**
   * Returns the candidate's last log index.
   *
   * @return The candidate's last log index.
   */
  public Long logIndex() {
    return logIndex;
  }

  /**
   * Returns the candidate's last log term.
   *
   * @return The candidate's last log term.
   */
  public Long logTerm() {
    return logTerm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, term, candidate, logIndex, logTerm);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof VoteRequest) {
      VoteRequest request = (VoteRequest) object;
      return request.uri.equals(uri)
        && request.term == term
        && request.candidate.equals(candidate)
        && request.logIndex.equals(logIndex)
        && request.logTerm.equals(logTerm);
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
    protected Builder() {
      this(new VoteRequest());
    }

    protected Builder(VoteRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The vote request builder.
     */
    public Builder withTerm(long term) {
      request.term = Assert.arg(term, term > 0, "term must be greater than zero");
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param candidate The request candidate.
     * @return The vote request builder.
     */
    public Builder withCandidate(String candidate) {
      request.candidate = Assert.isNotNull(candidate, "candidate");
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The vote request builder.
     */
    public Builder withLogIndex(Long index) {
      request.logIndex = Assert.index(index, index == null || index > 0, "index must be greater than zero");
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The vote request builder.
     */
    public Builder withLogTerm(Long term) {
      request.logTerm = Assert.arg(term, term == null || term > 0, "term must be greater than zero");
      return this;
    }

    @Override
    public VoteRequest build() {
      super.build();
      Assert.isNotNull(request.candidate, "candidate");
      Assert.arg(request.term, request.term > 0, "term must be greater than zero");
      Assert.index(request.logIndex, request.logIndex == null || request.logIndex > 0, "index must be greater than zero");
      Assert.arg(request.logTerm, request.logTerm == null || request.logTerm > 0, "term must be greater than zero");
      Assert.arg(null, (request.logIndex == null && request.logTerm == null) || (request.logIndex != null && request.logTerm != null), "log index and term must both be null or neither be null");
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
