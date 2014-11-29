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
package net.kuujo.copycat.protocol;

/**
 * Protocol response type.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ResponseType {
  private Class<? extends Response> type;
  private int id;

  public ResponseType() {
  }

  public ResponseType(Class<? extends Response> type, int id) {
    this.type = type;
    this.id = id;
  }

  /**
   * Returns the response type class.
   *
   * @return The response type class.
   */
  public Class<? extends Response> type() {
    return type;
  }

  /**
   * Returns the response type ID.
   *
   * @return The response type ID.
   */
  public int id() {
    return id;
  }

  @Override
  public String toString() {
    return String.format("ResponseType[type=%s, id=%d]", type, id);
  }

}
