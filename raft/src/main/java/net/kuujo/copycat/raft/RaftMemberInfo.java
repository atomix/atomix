/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft;

import java.util.HashSet;
import java.util.Set;

/**
 * Raft cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftMemberInfo {
  private static final int FAILURE_LIMIT = 3;
  private Type type;
  private Status status;
  private long changed;
  private String uri;
  private long version = 1;
  private Long index;
  private Set<String> failures = new HashSet<>();

  /**
   * Raft member type.<p>
   *
   * The member type indicates how cluster members behave in terms of joining and leaving the cluster and how the
   * members participate in log replication. {@link Type#ACTIVE} members are full voting members of the cluster that
   * participate in Copycat's consensus protocol. {@link Type#PASSIVE} members may join and leave the cluster at will
   * without impacting the availability of a resource and receive only committed log entries via a gossip protocol.
   */
  public static enum Type {

    /**
     * Indicates that the member is a passive, non-voting member of the cluster.
     */
    PASSIVE,

    /**
     * Indicates that the member is attempting to join the cluster.
     */
    PROMOTABLE,

    /**
     * Indicates that the member is an active voting member of the cluster.
     */
    ACTIVE

  }

  /**
   * Raft member status.<p>
   *
   * The member status indicates how a given member is perceived by the local node. Members can be in one of three states
   * at any given time, {@link net.kuujo.copycat.raft.RaftMemberInfo.Status#ALIVE}, {@link net.kuujo.copycat.raft.RaftMemberInfo.Status#SUSPICIOUS},
   * and {@link net.kuujo.copycat.raft.RaftMemberInfo.Status#DEAD}. Member states are changed according to the local
   * node's ability to communicate with a given member. All members begin with an {@link net.kuujo.copycat.raft.RaftMemberInfo.Status#ALIVE}
   * status upon joining the cluster. If the member appears to be unreachable, its status will be changed to
   * {@link net.kuujo.copycat.raft.RaftMemberInfo.Status#SUSPICIOUS}, indicating that it may have left the cluster or died.
   * Once enough other nodes in the cluster agree that the suspicious member appears to be dead, the status will be changed
   * to {@link net.kuujo.copycat.raft.RaftMemberInfo.Status#DEAD} and the member will ultimately be removed from the cluster configuration.
   */
  public static enum Status {

    /**
     * Indicates that the member is considered to be dead.
     */
    DEAD,

    /**
     * Indicates that the member is suspicious and is unreachable by at least one other member.
     */
    SUSPICIOUS,

    /**
     * Indicates that the member is alive and reachable.
     */
    ALIVE

  }

  public RaftMemberInfo() {
  }

  public RaftMemberInfo(String uri, Type type, Status status) {
    this(uri, type, status, 1);
  }

  public RaftMemberInfo(String uri, Type type, Status status, long version) {
    this.uri = uri;
    this.type = type;
    this.status = status;
    this.version = version;
  }

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the member status.
   *
   * @return The member status.
   */
  public Status status() {
    return status;
  }

  /**
   * Returns the last time the member status changed.
   *
   * @return The last time the member status changed.
   */
  public long changed() {
    return changed;
  }

  /**
   * Returns the member URI.
   *
   * @return The member URI.
   */
  public String uri() {
    return uri;
  }

  /**
   * Returns the member version.
   *
   * @return The member version.
   */
  public long version() {
    return version;
  }

  /**
   * Sets the member version.
   *
   * @param version The member version.
   * @return The member info.
   */
  RaftMemberInfo version(long version) {
    this.version = version;
    return this;
  }

  /**
   * Returns the member's last log index.
   *
   * @return The member's last log index.
   */
  public Long index() {
    return index;
  }

  /**
   * Sets the member index.
   *
   * @param index The member's last log index.
   * @return The member info.
   */
  RaftMemberInfo index(Long index) {
    this.index = index;
    return this;
  }

  /**
   * Marks a successful gossip with the member.
   *
   * @return The member info.
   */
  public RaftMemberInfo succeed() {
    if (type == Type.PASSIVE && status != Status.ALIVE) {
      failures.clear();
      status = Status.ALIVE;
      changed = System.currentTimeMillis();
    }
    return this;
  }

  /**
   * Marks a failure in the member.
   *
   * @param uri The URI recording the failure.
   * @return The member info.
   */
  public RaftMemberInfo fail(String uri) {
    // If the member is a passive member, add the failure to the failures set and change the status. If the current
    // status is ACTIVE then change the status to SUSPICIOUS. If the current status is SUSPICIOUS and the number of
    // failures from *unique* nodes is equal to or greater than the failure limit then change the status to DEAD.
    if (type == Type.PASSIVE) {
      failures.add(uri);
      if (status == Status.ALIVE) {
        status = Status.SUSPICIOUS;
        changed = System.currentTimeMillis();
      } else if (status == Status.SUSPICIOUS) {
        if (failures.size() >= FAILURE_LIMIT) {
          status = Status.DEAD;
          changed = System.currentTimeMillis();
        }
      }
    }
    return this;
  }

  /**
   * Updates the member info.
   *
   * @param info The member info to update.
   */
  public void update(RaftMemberInfo info) {
    // If the given version is greater than the current version then update the member status.
    if (info.version > this.version) {
      this.type = info.type;
      this.version = info.version;
      this.index = info.index;

      // Any time the version is incremented, clear failures for the previous version.
      this.failures.clear();

      // Only passive member types can experience status changes. Active members are always alive.
      if (this.type == Type.PASSIVE) {
        // If the status changed then update the status and set the last changed time. This can be used to clean
        // up status related to old members after some period of time.
        if (this.status != info.status) {
          changed = System.currentTimeMillis();
        }
        this.status = info.status;
      }
    } else if (info.version == this.version) {
      this.type = info.type;
      if (info.status == Status.SUSPICIOUS) {
        // If the given version is the same as the current version then update failures. If the member has experienced
        // FAILURE_LIMIT failures then transition the member's status to DEAD.
        this.failures.addAll(info.failures);
        if (this.failures.size() >= FAILURE_LIMIT) {
          this.status = Status.DEAD;
          changed = System.currentTimeMillis();
        }
      } else if (info.status == Status.DEAD) {
        this.status = Status.DEAD;
        changed = System.currentTimeMillis();
      }
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RaftMemberInfo) {
      RaftMemberInfo member = (RaftMemberInfo) object;
      return member.uri.equals(uri)
        && member.type == type
        && member.status == status
        && member.version == version
        && member.index == index;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 17;
    hashCode = 37 * hashCode + uri.hashCode();
    hashCode = 37 * hashCode + type.hashCode();
    hashCode = 37 * hashCode + status.hashCode();
    if (index != null) hashCode = 37 * hashCode + (int)(index ^ (index >>> 32));
    hashCode = 37 * hashCode + (int)(version ^ (version >>> 32));
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("RaftMember[uri=%s, type=%s, state=%s, version=%d, index=%s]", uri, type, status, version, index);
  }

}
