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
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.cluster.Member;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Cluster member info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemberInfo implements Serializable {
  private static final int FAILURE_LIMIT = 3;
  private Member.Type type;
  private Member.State state;
  private long changed;
  private String uri;
  private long version = 1;
  private Set<String> failures = new HashSet<>();

  public MemberInfo() {
  }

  public MemberInfo(String uri, Member.Type type, Member.State state) {
    this(uri, type, state, 1);
  }

  public MemberInfo(String uri, Member.Type type, Member.State state, long version) {
    this.uri = uri;
    this.type = type;
    this.state = state;
    this.version = version;
  }

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  public Member.Type type() {
    return type;
  }

  /**
   * Returns the member state.
   *
   * @return The member state.
   */
  public Member.State state() {
    return state;
  }

  /**
   * Returns the last time the member state changed.
   *
   * @return The last time the member state changed.
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
  public MemberInfo version(long version) {
    this.version = version;
    return this;
  }

  /**
   * Marks a successful gossip with the member.
   *
   * @return The member info.
   */
  public MemberInfo succeed() {
    if (type == Member.Type.PASSIVE && state != Member.State.ALIVE) {
      failures.clear();
      state = Member.State.ALIVE;
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
  public MemberInfo fail(String uri) {
    // If the member is a passive member, add the failure to the failures set and change the state. If the current
    // state is ACTIVE then change the state to SUSPICIOUS. If the current state is SUSPICIOUS and the number of
    // failures from *unique* nodes is equal to or greater than the failure limit then change the state to DEAD.
    if (type == Member.Type.PASSIVE) {
      failures.add(uri);
      if (state == Member.State.ALIVE) {
        state = Member.State.SUSPICIOUS;
        changed = System.currentTimeMillis();
      } else if (state == Member.State.SUSPICIOUS) {
        if (failures.size() >= FAILURE_LIMIT) {
          state = Member.State.DEAD;
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
  public void update(MemberInfo info) {
    // If the given version is greater than the current version then update the member state.
    if (info.version > this.version) {
      this.version = info.version;

      // Any time the version is incremented, clear failures for the previous version.
      this.failures.clear();

      // Only passive member types can experience state changes. Active members are always alive.
      if (this.type == Member.Type.PASSIVE) {
        // If the state changed then update the state and set the last changed time. This can be used to clean
        // up state related to old members after some period of time.
        if (this.state != info.state) {
          changed = System.currentTimeMillis();
        }
        this.state = info.state;
      }
    } else if (info.version == this.version) {
      if (info.state == Member.State.SUSPICIOUS) {
        // If the given version is the same as the current version then update failures. If the member has experienced
        // FAILURE_LIMIT failures then transition the member's state to DEAD.
        this.failures.addAll(info.failures);
        if (this.failures.size() >= FAILURE_LIMIT) {
          this.state = Member.State.DEAD;
          changed = System.currentTimeMillis();
        }
      } else if (info.state == Member.State.DEAD) {
        this.state = Member.State.DEAD;
        changed = System.currentTimeMillis();
      }
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof MemberInfo) {
      MemberInfo member = (MemberInfo) object;
      return member.uri.equals(uri)
        && member.type == type
        && member.state == state
        && member.version == version;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, type, state, version);
  }

  @Override
  public String toString() {
    return String.format("MemberInfo[uri=%s, type=%s, state=%s, version=%d]", uri, type, state, version);
  }

}
