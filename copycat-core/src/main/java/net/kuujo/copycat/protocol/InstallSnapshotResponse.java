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
 * Install snapshot response.<p>
 *
 * The install response is sent in response to an install request.
 * The response indicates the receiving node's current term as well
 * as whether the installation of the snapshot was successful.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InstallSnapshotResponse extends Response {
  private static final long serialVersionUID = -2857849500915162283L;
  private long term;
  private boolean succeeded;

  public InstallSnapshotResponse(String id, long term, boolean succeeded) {
    super(id, Status.OK);
    this.term = term;
    this.succeeded = succeeded;
  }

  public InstallSnapshotResponse(String id, Throwable t) {
    super(id, Status.ERROR, t);
  }

  public InstallSnapshotResponse(String id, String error) {
    super(id, Status.ERROR, error);
  }

  /**
   * Returns the response term.
   *
   * @return The response term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the installation succeeded.
   *
   * @return Indicates whether the installation succeeded.
   */
  public boolean succeeded() {
    return succeeded;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, succeeded=%b]", getClass().getSimpleName(), term, succeeded);
  }

}
