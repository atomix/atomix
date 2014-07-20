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
import net.kuujo.copycat.serializer.SerializerFactory;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * A poll request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PollRequest extends Request<PollResponse> {
  private static final long serialVersionUID = -5282035829185619414L;
  private static final Serializer serializer = SerializerFactory.getSerializer();
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

  public static PollRequest fromJson(byte[] json) {
    return serializer.readValue(json, PollRequest.class);
  }

  public static PollRequest fromJson(byte[] json, AsyncCallback<PollResponse> callback) {
    PollRequest request = serializer.readValue(json, PollRequest.class);
    request.setResponseCallback(callback);
    return request;
  }

  public static byte[] toJson(PollRequest request) {
    return serializer.writeValue(request);
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
   * Responds to the vote request.
   * 
   * @param term The responding node's term.
   * @param voteGranted Indicates whether the vote was granted.
   */
  public void respond(long term, boolean voteGranted) {
    super.respond(new PollResponse(term, voteGranted));
  }

  /**
   * Responds to the request with an error.
   *
   * @param t The error that occurred.
   */
  public void respond(Throwable t) {
    super.respond(new PollResponse(t));
  }

  /**
   * Responds to the request with an error message.
   *
   * @param message The error message.
   */
  public void respond(String message) {
    super.respond(new PollResponse(message));
  }

  @Override
  public String toString() {
    return String.format("PollRequest[term=%s, candidate=%s, lastLogIndex=%s, lastLogTerm=%s]", term, candidate, lastLogIndex, lastLogTerm);
  }

}
