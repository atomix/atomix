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
package net.kuujo.mimeo.protocol;

import net.kuujo.mimeo.serializer.Serializer;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * A ping request.
 * 
 * @author Jordan Halterman
 */
public class PingRequest extends Request {
  private static final Serializer serializer = Serializer.getInstance();
  private long term;
  private String leader;

  public PingRequest() {
  }

  public PingRequest(long term, String leader) {
    this.term = term;
    this.leader = leader;
  }

  public static PingRequest fromJson(JsonObject json) {
    return serializer.deserialize(json, PingRequest.class);
  }

  public static PingRequest fromJson(JsonObject json, Message<JsonObject> message) {
    return serializer.deserialize(json, PingRequest.class).setMessage(message);
  }

  public static JsonObject toJson(PingRequest request) {
    return serializer.serialize(request);
  }

  private PingRequest setMessage(Message<JsonObject> message) {
    this.message = message;
    return this;
  }

  /**
   * Returns the requesting node's current term.
   * 
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  public String leader() {
    return leader;
  }

  public void reply(long term) {
    reply(new JsonObject().putNumber("term", term));
  }

}
