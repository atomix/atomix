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

import java.util.Set;

/**
 * Install request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InstallRequest extends Request<InstallResponse> {
  private static final long serialVersionUID = 1475758554918256823L;
  private long term;
  private String leader;
  private long snapshotIndex;
  private long snapshotTerm;
  private Set<String> cluster;
  private byte[] data;
  private boolean complete;

  public InstallRequest(long term, String leader, long snapshotIndex, long snapshotTerm, Set<String> cluster, byte[] data, boolean complete) {
    this.term = term;
    this.leader = leader;
    this.snapshotIndex = snapshotIndex;
    this.snapshotTerm = snapshotTerm;
    this.cluster = cluster;
    this.data = data;
    this.complete = complete;
  }

  /**
   * Returns the leader's current term.
   *
   * @return The leader's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the leader sending the snapshot.
   *
   * @return The identifier of the leader sending the snapshot.
   */
  public String leader() {
    return leader;
  }

  /**
   * Returns the snapshot's index in the log.
   *
   * @return The index of the snapshot entry in the log.
   */
  public long snapshotIndex() {
    return snapshotIndex;
  }

  /**
   * Returns the snapshot's term in the log.
   *
   * @return The term of the snapshot entry in the log.
   */
  public long snapshotTerm() {
    return snapshotTerm;
  }

  /**
   * Returns the snapshot's cluster membership.
   *
   * @return The snapshot's cluster configuration.
   */
  public Set<String> cluster() {
    return cluster;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
  }

  /**
   * Returns a boolean indicating whether the snapshot is complete.
   *
   * @return Indicates whether all the data for the snapshot has been sent.
   */
  public boolean complete() {
    return complete;
  }

  /**
   * Responds to the request.
   *
   * @param term The current term.
   * @param succeeded Whether the installation was successful.
   */
  public void respond(long term, boolean succeeded) {
    super.respond(new InstallResponse(term, succeeded));
  }

  /**
   * Responds to the request.
   *
   * @param t A response exception.
   */
  public void respond(Throwable t) {
    super.respond(new InstallResponse(t));
  }

  /**
   * Responds to the request with an error.
   *
   * @param error The error message.
   */
  public void respond(String error) {
    super.respond(new InstallResponse(error));
  }

}
