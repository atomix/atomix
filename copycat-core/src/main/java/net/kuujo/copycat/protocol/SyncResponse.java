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
 * Sync response.<p>
 *
 * The Sync response is sent in response to an append request
 * from the cluster leader. The response simply indicates whether the
 * synchronization was successful.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncResponse extends Response {
  private static final long serialVersionUID = 8897167973047662707L;
  private long term;
  private boolean succeeded;
  private long lastLogIndex;

  public SyncResponse() {
  }

  public SyncResponse(Object id, long term, boolean succeeded, long lastLogIndex) {
    super(id, Status.OK);
    this.term = term;
    this.succeeded = succeeded;
    this.lastLogIndex = lastLogIndex;
  }

  public SyncResponse(Object id, Throwable t) {
    super(id, Status.ERROR, t);
  }

  public SyncResponse(Object id, String message) {
    super(id, Status.ERROR, message);
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
   * Returns a boolean indicating whether the sync was successful.
   * 
   * @return Indicates whether the sync was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  public long lastLogIndex() {
    return lastLogIndex;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncResponse) {
      SyncResponse response = (SyncResponse) object;
      return response.id().equals(id()) && response.status().equals(status()) && response.term == term && response.succeeded == succeeded && response.lastLogIndex == lastLogIndex;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id().hashCode();
    hashCode = 37 * hashCode + status().hashCode();
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    hashCode = 37 * hashCode + (succeeded ? 1 : 0);
    hashCode = 37 * hashCode + (int)(lastLogIndex ^ (lastLogIndex >>> 32));
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, term=%d, succeeded=%b]", getClass().getSimpleName(), id(), term, succeeded);
  }

}
