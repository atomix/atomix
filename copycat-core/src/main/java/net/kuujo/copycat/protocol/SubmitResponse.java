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

import java.util.Map;

import net.kuujo.copycat.util.serializer.Serializer;

/**
 * A submit response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitResponse extends Response {
  private static final Serializer serializer = Serializer.getInstance();
  private Map<String, Object> result;

  public SubmitResponse() {
  }

  public SubmitResponse(Map<String, Object> result) {
    super(Status.OK);
    this.result = result;
  }

  public SubmitResponse(Throwable t) {
    super(Status.ERROR, t);
  }

  public SubmitResponse(String message) {
    super(Status.ERROR, message);
  }

  public static SubmitResponse fromJson(byte[] json) {
    return serializer.readValue(json, SubmitResponse.class);
  }

  public static byte[] toJson(SubmitResponse response) {
    return serializer.writeValue(response);
  }

  /**
   * Returns the command result.
   *
   * @return The command execution result.
   */
  public Map<String, Object> result() {
    return result;
  }

  @Override
  public String toString() {
    return String.format("SubmitResponse[result=%s]", result);
  }

}
