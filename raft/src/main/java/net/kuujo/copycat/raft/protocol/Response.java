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

/**
 * Protocol response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Response {

  /**
   * Response status.
   */
  public static enum Status {

    /**
     * Indicates a successful response status.
     */
    OK,

    /**
     * Indicates a response containing an error.
     */
    ERROR
  }

  /**
   * Returns the response member.
   *
   * @return The member to which to send the response.
   */
  String uri();

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
  Throwable error();

  /**
   * Response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  static interface Builder<T extends Builder<T, U>, U extends Response> {

    /**
     * Sets the response member.
     *
     * @param member The member to which to send the response.
     * @return The response builder.
     */
    T withUri(String member);

    /**
     * Sets the response status.
     *
     * @param status The response status.
     * @return The response builder.
     */
    T withStatus(Status status);

    /**
     * Sets the response error.
     *
     * @param error The response error.
     * @return The response builder.
     */
    T withError(Throwable error);

    /**
     * Builds the request.
     *
     * @return The built request.
     */
    U build();

  }

}
