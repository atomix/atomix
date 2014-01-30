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

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.mimeo.serializer.Serializer;

/**
 * A poll response.
 *
 * @author Jordan Halterman
 */
public class PollResponse implements Response {
  private static final Serializer serializer = Serializer.getInstance();
  private long term;
  private boolean voteGranted;

  public PollResponse() {
  }

  public PollResponse(long term, boolean voteGranted) {
    this.term = term;
    this.voteGranted = voteGranted;
  }

  public static PollResponse fromJson(JsonObject json) {
    return serializer.deserialize(json, PollResponse.class);
  }

  public static PollResponse fromJson(Message<JsonObject> message) {
    return serializer.deserialize(message.body(), PollResponse.class);
  }

  public static JsonObject toJson(PollResponse response) {
    return serializer.serialize(response);
  }

  /**
   * Returns the responding node's current term.
   *
   * @return
   *   The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the vote was granted.
   *
   * @return
   *   Indicates whether the vote was granted.
   */
  public boolean voteGranted() {
    return voteGranted;
  }

}
