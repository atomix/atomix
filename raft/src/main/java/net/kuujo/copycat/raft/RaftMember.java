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

import net.kuujo.copycat.util.internal.Assert;

import java.util.HashSet;
import java.util.Set;

/**
 * Raft cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftMember {
  private static final int FAILURE_LIMIT = 3;
  private Type type;
  private Status status;
  private long changed;
  private String id;
  private String address;
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
   * at any given time, {@link RaftMember.Status#ALIVE}, {@link RaftMember.Status#SUSPICIOUS},
   * and {@link RaftMember.Status#DEAD}. Member states are changed according to the local
   * node's ability to communicate with a given member. All members begin with an {@link RaftMember.Status#ALIVE}
   * status upon joining the cluster. If the member appears to be unreachable, its status will be changed to
   * {@link RaftMember.Status#SUSPICIOUS}, indicating that it may have left the cluster or died.
   * Once enough other nodes in the cluster agree that the suspicious member appears to be dead, the status will be changed
   * to {@link RaftMember.Status#DEAD} and the member will ultimately be removed from the cluster configuration.
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

  public RaftMember() {
  }

  public RaftMember(String id, String address, Type type) {
    this(id, address, type, Status.ALIVE, 1);
  }

  RaftMember(String id, String address, Type type, Status status) {
    this(id, address, type, status, 1);
  }

  RaftMember(String id, String address, Type type, Status status, long version) {
    this.id = Assert.notNull(id, "id");
    this.address = Assert.notNull(address, "address");
    this.type = Assert.notNull(type, "type");
    this.status = Assert.notNull(status, "status");
    this.version = Assert.arg(version, version >= 0, "version must be positive");
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public String id() {
    return id;
  }

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  public String address() {
    return address;
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
  RaftMember version(long version) {
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
  RaftMember index(Long index) {
    this.index = index;
    return this;
  }

  /**
   * Marks a successful gossip with the member.
   *
   * @return The member info.
   */
  RaftMember succeed() {
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
  RaftMember fail(String uri) {
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
   * @return Indicates whether the member's state was updated.
   */
  boolean update(RaftMember info) {
    // If the given version is greater than the current version then update the member status.
    boolean updated = false;
    if (info.version > this.version) {
      this.type = info.type;
      this.version = info.version;
      this.address = info.address;
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
      updated = true;
    } else if (info.version == this.version) {
      // If this is a new type then update the type and return true.
      if (this.type != info.type) {
        this.type = info.type;
        updated = true;
      }

      if (info.status == Status.SUSPICIOUS) {
        // If the given version is the same as the current version then update failures. If the member has experienced
        // FAILURE_LIMIT failures then transition the member's status to DEAD.
        this.failures.addAll(info.failures);
        if (this.failures.size() >= FAILURE_LIMIT) {
          this.status = Status.DEAD;
          changed = System.currentTimeMillis();
          updated = true;
        }
      } else if (info.status == Status.DEAD && this.status != Status.DEAD) {
        this.status = Status.DEAD;
        changed = System.currentTimeMillis();
        updated = true;
      }
    }
    return updated;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RaftMember) {
      RaftMember member = (RaftMember) object;
      return member.id.equals(id)
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
    hashCode = 37 * hashCode + id.hashCode();
    hashCode = 37 * hashCode + type.hashCode();
    hashCode = 37 * hashCode + status.hashCode();
    if (index != null) hashCode = 37 * hashCode + (int)(index ^ (index >>> 32));
    hashCode = 37 * hashCode + (int)(version ^ (version >>> 32));
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("RaftMember[id=%s, type=%s, state=%s, version=%d, index=%s]", id, type, status, version, index);
  }

}
