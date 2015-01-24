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
package net.kuujo.copycat.protocol.rpc;

/**
 * Protocol request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Request {

  /**
   * Returns the request ID.
   *
   * @return The unique request ID.
   */
  Object id();

  /**
   * Returns the request member.
   *
   * @return The request member.
   */
  String uri();

  /**
   * Request builder.
   *
   * @param <T> The builder type.
   * @param <U> The request type.
   */
  static interface Builder<T extends Builder<T, U>, U extends Request> {

    /**
     * Sets the request ID.
     *
     * @param id The request ID.
     * @return The request builder.
     */
    T withId(Object id);

    /**
     * Sets the request member.
     *
     * @param uri The member to which to send the request.
     * @return The request builder.
     */
    T withUri(String uri);

    /**
     * Builds the request.
     *
     * @return The built request.
     */
    U build();

  }

}
