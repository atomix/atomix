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

import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.io.util.ReferenceCounted;

/**
 * Protocol request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Request<REQUEST extends Request<REQUEST>> extends ReferenceCounted<REQUEST>, Writable {

  /**
   * Request type.
   */
  public static enum Type {

    /**
     * Register client request.
     */
    REGISTER,

    /**
     * Keep alive request.
     */
    KEEP_ALIVE,

    /**
     * Join request.
     */
    JOIN,

    /**
     * Promote request.
     */
    PROMOTE,

    /**
     * Demote request.
     */
    DEMOTE,

    /**
     * Leave request.
     */
    LEAVE,

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
     * Submit request.
     */
    SUBMIT
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
   * @param <BUILDER> The builder type.
   * @param <REQUEST> The request type.
   */
  static interface Builder<BUILDER extends Builder<BUILDER, REQUEST>, REQUEST extends Request> {

    /**
     * Builds the request.
     *
     * @return The built request.
     */
    REQUEST build();

  }

}
