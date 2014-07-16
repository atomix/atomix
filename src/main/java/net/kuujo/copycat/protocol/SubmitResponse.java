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

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

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
    this.result = result;
  }

  public SubmitResponse(JsonObject result) {
    this.result = result.toMap();
  }

  public static SubmitResponse fromJson(JsonObject json) {
    return serializer.readObject(json, SubmitResponse.class);
  }

  public static SubmitResponse fromJson(Message<JsonObject> message) {
    return serializer.readObject(message.body(), SubmitResponse.class);
  }

  public static JsonObject toJson(SubmitResponse response) {
    return serializer.writeObject(response);
  }

  /**
   * Returns the command result.
   *
   * @return The command execution result.
   */
  public JsonObject result() {
    return result != null ? new JsonObject(result) : new JsonObject();
  }

  @Override
  public String toString() {
    return String.format("SubmitResponse[result=%s]", result);
  }

}
