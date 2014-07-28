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
 * The sync response is sent in response to a sync request from
 * the cluster leader. The response simply indicates whether the
 * synchronization was successful.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncResponse extends Response {
  private static final long serialVersionUID = 8897167973047662707L;
  private long term;
  private boolean succeeded;

  public SyncResponse() {
  }

  public SyncResponse(long term, boolean succeeded) {
    super(Status.OK);
    this.term = term;
    this.succeeded = succeeded;
  }

  public SyncResponse(Throwable t) {
    super(Status.ERROR, t);
  }

  public SyncResponse(String message) {
    super(Status.ERROR, message);
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

  @Override
  public String toString() {
    return String.format("SyncResponse[term=%s, succeeded=%s]", term, succeeded);
  }

}
