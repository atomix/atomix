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
 * Protocol request type.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class RequestType {
  private Class<? extends Request> type;
  private int id;

  public RequestType(Class<? extends Request> type, int id) {
    this.type = type;
    this.id = id;
  }

  /**
   * Returns the request type class.
   *
   * @return The request type class.
   */
  public Class<? extends Request> type() {
    return type;
  }

  /**
   * Returns the request type ID.
   *
   * @return The request type ID.
   */
  public int id() {
    return id;
  }

  @Override
  public String toString() {
    return String.format("RequestType[type=%s, id=%d]", type, id);
  }

}
