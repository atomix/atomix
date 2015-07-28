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

import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.util.ReferenceCounted;

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
     * Join request.
     */
    JOIN,

    /**
     * Leave request.
     */
    LEAVE,

    /**
     * Register client request.
     */
    REGISTER,

    /**
     * Keep alive request.
     */
    KEEP_ALIVE,

    /**
     * Append request.
     */
    APPEND,

    /**
     * Poll request.
     */
    POLL,

    /**
     * Vote request.
     */
    VOTE,

    /**
     * Command request.
     */
    COMMAND,

    /**
     * Query request.
     */
    QUERY,

    /**
     * Publish request.
     */
    PUBLISH

  }

  /**
   * Returns the request type.
   *
   * @return The request type.
   */
  Type type();

  /**
   * Request builder.
   *
   * @param <T> The builder type.
   */
  static abstract class Builder<T extends Builder<T, U>, U extends Request> extends net.kuujo.copycat.Builder<U> {
    protected Builder(BuilderPool pool) {
      super(pool);
    }
  }

}
