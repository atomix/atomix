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

import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.util.ReferenceCounted;

/**
 * Protocol request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Request<T extends Request<T>> extends ReferenceCounted<T>, CopycatSerializable {

  /**
   * Request type.
   */
  public static enum Type {

    /**
     * Append request.
     */
    APPEND,

    /**
     * Sync request.
     */
    SYNC,

    /**
     * Poll request.
     */
    POLL,

    /**
     * Vote request.
     */
    VOTE,

    /**
     * Write request.
     */
    WRITE,

    /**
     * Read request.
     */
    READ,

    /**
     * Delete request.
     */
    DELETE
  }

  /**
   * Returns the request type.
   *
   * @return The request type.
   */
  Type type();

  /**
   * Returns the request as an append request.
   *
   * @return An append request.
   */
  @SuppressWarnings("unchecked")
  default AppendRequest asAppendRequest() {
    return (AppendRequest) (T) this;
  }

  /**
   * Returns the request as a sync request.
   *
   * @return A sync request.
   */
  @SuppressWarnings("unchecked")
  default SyncRequest asSyncRequest() {
    return (SyncRequest) (T) this;
  }

  /**
   * Returns the request as a write request.
   *
   * @return A write request.
   */
  @SuppressWarnings("unchecked")
  default WriteRequest asWriteRequest() {
    return (WriteRequest) (T) this;
  }

  /**
   * Returns the request as a read request.
   *
   * @return A read request.
   */
  @SuppressWarnings("unchecked")
  default ReadRequest asReadRequest() {
    return (ReadRequest) (T) this;
  }

  /**
   * Returns the request as a delete request.
   *
   * @return A delete request.
   */
  @SuppressWarnings("unchecked")
  default DeleteRequest asDeleteRequest() {
    return (DeleteRequest) (T) this;
  }

  /**
   * Returns the request as a poll request.
   *
   * @return A poll request.
   */
  @SuppressWarnings("unchecked")
  default PollRequest asPollRequest() {
    return (PollRequest) (T) this;
  }

  /**
   * Returns the request as a vote request.
   *
   * @return A vote request.
   */
  @SuppressWarnings("unchecked")
  default VoteRequest asVoteRequest() {
    return (VoteRequest) (T) this;
  }

  /**
   * Request builder.
   *
   * @param <T> The builder type.
   * @param <U> The request type.
   */
  static interface Builder<T extends Builder<T, U>, U extends Request> {

    /**
     * Builds the request.
     *
     * @return The built request.
     */
    U build();

  }

}
