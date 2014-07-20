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

/**
 * A ping request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PingRequest extends Request<PingResponse> {
  private static final long serialVersionUID = -4293135418334841156L;
  private long term;
  private String leader;

  public PingRequest() {
  }

  public PingRequest(long term, String leader) {
    this.term = term;
    this.leader = leader;
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
   * Returns the requesting node's current leader.
   *
   * @return The requesting node's current known leader.
   */
  public String leader() {
    return leader;
  }

  /**
   * Responds to the request.
   *
   * @param term The responding node's current term.
   */
  public void respond(long term) {
    super.respond(new PingResponse(term));
  }

  /**
   * Responds to the request with an error.
   *
   * @param t The error that occurred.
   */
  public void respond(Throwable t) {
    super.respond(new PingResponse(t));
  }

  /**
   * Responds to the request with an error message.
   *
   * @param message The error message.
   */
  public void respond(String message) {
    super.respond(new PingResponse(message));
  }

  @Override
  public String toString() {
    return String.format("PingRequest[term=%s, leader=%s]", term, leader);
  }

}
