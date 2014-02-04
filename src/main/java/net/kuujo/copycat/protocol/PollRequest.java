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

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import net.kuujo.copycat.serializer.Serializer;

/**
 * A poll request.
 * 
 * @author Jordan Halterman
 */
public class PollRequest extends Request {
  private static final Serializer serializer = Serializer.getInstance();
  private long term;
  private String candidate;
  private long lastLogIndex;
  private long lastLogTerm;

  public PollRequest() {
  }

  public PollRequest(long term, String candidate, long lastLogIndex, long lastLogTerm) {
    this.term = term;
    this.candidate = candidate;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
  }

  public static PollRequest fromJson(JsonObject json) {
    return serializer.deserialize(json, PollRequest.class);
  }

  public static PollRequest fromJson(JsonObject json, Message<JsonObject> message) {
    return serializer.deserialize(json, PollRequest.class).setMessage(message);
  }

  public static JsonObject toJson(PollRequest request) {
    return serializer.serialize(request);
  }

  private PollRequest setMessage(Message<JsonObject> message) {
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

  /**
   * Returns the candidate's address.
   * 
   * @return The candidate's address.
   */
  public String candidate() {
    return candidate;
  }

  /**
   * Returns the candidate's last log index.
   * 
   * @return The candidate's last log index.
   */
  public long lastLogIndex() {
    return lastLogIndex;
  }

  /**
   * Returns the candidate's last log term.
   * 
   * @return The candidate's last log term.
   */
  public long lastLogTerm() {
    return lastLogTerm;
  }

  /**
   * Replies to the vote request.
   * 
   * @param term The responding node's term.
   * @param voteGranted Indicates whether the vote was granted.
   */
  public void reply(long term, boolean voteGranted) {
    reply(new JsonObject().putNumber("term", term).putBoolean("voteGranted", voteGranted));
  }

}
