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

import net.kuujo.copycat.serializer.Serializer;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * A ping response.
 * 
 * @author Jordan Halterman
 */
public class PingResponse implements Response {
  private static final Serializer serializer = Serializer.getInstance();
  private long term;

  public PingResponse() {
  }

  public PingResponse(long term) {
    this.term = term;
  }

  public static PingResponse fromJson(JsonObject json) {
    return serializer.deserialize(json, PingResponse.class);
  }

  public static PingResponse fromJson(Message<JsonObject> message) {
    return serializer.deserialize(message.body(), PingResponse.class);
  }

  public static JsonObject toJson(PingResponse response) {
    return serializer.serialize(response);
  }

  /**
   * Returns the requesting node's current term.
   * 
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

}
