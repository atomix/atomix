/*
 * Copyright 2014 the original author or authors.
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

import java.io.Serializable;

/**
 * A request response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SuppressWarnings("serial")
public abstract class Response implements Serializable {

  /**
   * Response status.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static enum Status {

    /**
     * Indicates a successful response status.
     */
    OK,

    /**
     * Indicates a response containing an error.
     */
    ERROR;

  }

  private final Object id;
  private final Status status;
  private final String error;

  protected Response() {
    this.id = null;
    this.status = Status.OK;
    this.error = null;
  }

  protected Response(Object id, Status status) {
    this.id = id;
    this.status = status;
    this.error = null;
  }

  protected Response(Object id, Status status, Throwable t) {
    this.id = id;
    this.status = status;
    this.error = t.getMessage();
  }

  protected Response(Object id, Status status, String error) {
    this.id = id;
    this.status = status;
    this.error = error;
  }

  /**
   * Returns the response correlation ID.
   *
   * @return The response correlation ID.
   */
  public Object id() {
    return id;
  }

  /**
   * Returns the response status.
   *
   * @return The response status.
   */
  public Status status() {
    return status;
  }

  /**
   * Returns the response error.
   *
   * @return The response error if one exists.
   */
  public Throwable error() {
    return error != null ? new ResponseException(error) : null;
  }

}
