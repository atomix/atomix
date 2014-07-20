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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A request response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonAutoDetect(
    creatorVisibility=JsonAutoDetect.Visibility.NONE,
    fieldVisibility=JsonAutoDetect.Visibility.ANY,
    getterVisibility=JsonAutoDetect.Visibility.NONE,
    isGetterVisibility=JsonAutoDetect.Visibility.NONE,
    setterVisibility=JsonAutoDetect.Visibility.NONE
)
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

  private final Status status;
  private final String error;

  protected Response() {
    this.status = Status.OK;
    this.error = null;
  }

  protected Response(Status status) {
    this.status = status;
    this.error = null;
  }

  protected Response(Status status, Throwable t) {
    this.status = status;
    this.error = t.getMessage();
  }

  protected Response(Status status, String error) {
    this.status = status;
    this.error = error;
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
