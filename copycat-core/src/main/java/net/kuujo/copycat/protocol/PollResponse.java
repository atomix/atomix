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

/**
 * A poll response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PollResponse extends Response {
  private static final long serialVersionUID = 8188723576438876929L;
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private long term;
  private boolean voteGranted;

  public PollResponse() {
  }

  public PollResponse(long term, boolean voteGranted) {
    super(Status.OK);
    this.term = term;
    this.voteGranted = voteGranted;
  }

  public PollResponse(Throwable t) {
    super(Status.ERROR, t);
  }

  public PollResponse(String message) {
    super(Status.ERROR, message);
  }

  public static PollResponse fromJson(byte[] json) {
    return serializer.readValue(json, PollResponse.class);
  }

  public static byte[] toJson(PollResponse response) {
    return serializer.writeValue(response);
  }

  /**
   * Returns the responding node's current term.
   * 
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the vote was granted.
   * 
   * @return Indicates whether the vote was granted.
   */
  public boolean voteGranted() {
    return voteGranted;
  }

  @Override
  public String toString() {
    return String.format("PollResponse[term=%s, voteGranted=%s]", term, voteGranted);
  }

}
