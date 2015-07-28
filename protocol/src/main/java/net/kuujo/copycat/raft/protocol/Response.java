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

import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.util.ReferenceCounted;

/**
 * Protocol response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Response<T extends Response<T>> extends ReferenceCounted<T>, CopycatSerializable {

  /**
   * Response type.
   */
  public static enum Type {

    /**
     * Join response.
     */
    JOIN,

    /**
     * Leave response.
     */
    LEAVE,

    /**
     * Register client response.
     */
    REGISTER,

    /**
     * Keep alive response.
     */
    KEEP_ALIVE,

    /**
     * Append response.
     */
    APPEND,

    /**
     * Poll response.
     */
    POLL,

    /**
     * Vote response.
     */
    VOTE,

    /**
     * Command response.
     */
    COMMAND,

    /**
     * Query response.
     */
    QUERY,

    /**
     * Publish response.
     */
    PUBLISH

    }

  /**
   * Response status.
   */
  public static enum Status {

    /**
     * Indicates a successful response status.
     */
    OK(1),

    /**
     * Indicates a response containing an error.
     */
    ERROR(0);

    /**
     * Returns the status for the given identifier.
     *
     * @param id The status identifier.
     * @return The status for the given identifier.
     */
    public static Status forId(int id) {
      switch (id) {
        case 1:
          return OK;
        case 0:
          return ERROR;
      }
      throw new IllegalArgumentException("invalid status identifier: " + id);
    }

    private final byte id;

    private Status(int id) {
      this.id = (byte) id;
    }

    /**
     * Returns the status identifier.
     *
     * @return The status identifier.
     */
    public byte id() {
      return id;
    }
  }

  /**
   * Returns the response type.
   *
   * @return The response type.
   */
  Type type();

  /**
   * Returns the response status.
   *
   * @return The response status.
   */
  Status status();

  /**
   * Returns the response error if the response status is {@code Status.ERROR}
   *
   * @return The response error.
   */
  RaftError error();

  /**
   * Response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  static abstract class Builder<T extends Builder<T, U>, U extends Response> extends net.kuujo.copycat.util.Builder<U> {

    protected Builder(BuilderPool pool) {
      super(pool);
    }

    /**
     * Sets the response status.
     *
     * @param status The response status.
     * @return The response builder.
     */
    public abstract T withStatus(Status status);

    /**
     * Sets the response error.
     *
     * @param error The response error.
     * @return The response builder.
     */
    public abstract T withError(RaftError error);

  }

}
