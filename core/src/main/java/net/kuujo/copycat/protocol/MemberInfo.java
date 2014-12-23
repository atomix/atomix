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

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.util.Assert;

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

  /**
   * Returns a new member info builder.
   *
   * @return A new member info builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a new member info builder.
   *
   * @param member The member info to build.
   * @return A new member info builder.
   */
  public static Builder builder(MemberInfo member) {
    return new Builder(member);
  }

  private Member.Type type;
  private Member.State state;
  private String uri;
  private long version = 1;
  private Long index;
  private final Set<String> failures = new HashSet<>();

  private MemberInfo() {
  }

  public MemberInfo(String uri, Member.Type type, Member.State state) {
    this(uri, type, state, 1, null);
  }

  public MemberInfo(String uri, Member.Type type, Member.State state, long version, Long index) {
    this.uri = uri;
    this.type = type;
    this.state = state;
    this.version = version;
    this.index = index;
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
   * Returns the member index.
   *
   * @return The member index.
   */
  public Long index() {
    return index;
  }

  /**
   * Returns a set of all nodes that have recorded failures for this member.
   *
   * @return A set of all nodes that have recorded failures for this member.
   */
  public Set<String> failures() {
    return failures;
  }

  /**
   * Marks a successful gossip with the member.
   *
   * @return The member info.
   */
  public MemberInfo succeed() {
    if (type == Member.Type.LISTENER && state != Member.State.ALIVE) {
      failures.clear();
      state = Member.State.ALIVE;
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
    if (type == Member.Type.LISTENER) {
      failures.add(uri);
      if (state == Member.State.ALIVE) {
        state = Member.State.SUSPICIOUS;
      } else if (state == Member.State.SUSPICIOUS) {
        if (failures.size() >= 3) {
          state = Member.State.DEAD;
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
    if (info.version > this.version) {
      this.version = info.version;
      this.index = info.index;

      // Only passive member types can experience state changes.
      if (this.type == Member.Type.LISTENER) {
        // If the member is marked as alive then clear failures.
        if (info.state == Member.State.ALIVE) {
          this.failures.clear();
        }
        this.state = info.state;
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
        && member.version == version
        && member.index.equals(index);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, type, state, version, index);
  }

  @Override
  public String toString() {
    return String.format("MemberInfo[uri=%s, type=%s, state=%s, version=%d, index=%s]", uri, type, state, version, index);
  }

  /**
   * Member info builder.
   */
  public static class Builder {
    private final MemberInfo member;

    private Builder() {
      this(new MemberInfo());
    }

    private Builder(MemberInfo member) {
      this.member = member;
    }

    /**
     * Sets the member type.
     *
     * @param type The member type.
     * @return The member info builder.
     */
    public Builder withType(Member.Type type) {
      member.type = Assert.isNotNull(type, "type");
      return this;
    }

    /**
     * Sets the member state.
     *
     * @param state The member state.
     * @return The member info builder.
     */
    public Builder withState(Member.State state) {
      member.state = Assert.isNotNull(state, "state");
      return this;
    }

    /**
     * Sets the member URI.
     *
     * @param uri The member URI.
     * @return The member info builder.
     */
    public Builder withUri(String uri) {
      member.uri = Assert.isNotNull(uri, "uri");
      return this;
    }

    /**
     * Sets the member version.
     *
     * @param version The member version.
     * @return The member info builder.
     */
    public Builder withVersion(long version) {
      member.version = Assert.arg(version, version > 0, "version must be greater than zero");
      return this;
    }

    /**
     * Sets the member log commit index.
     *
     * @param index The member log commit index.
     * @return The member info builder.
     */
    public Builder withIndex(Long index) {
      member.index = Assert.arg(index, index == null || index > 0, "index must be greater than zero");
      return this;
    }

    /**
     * Builds the member.
     *
     * @return The built member.
     */
    public MemberInfo build() {
      Assert.isNotNull(member.type, "type");
      Assert.isNotNull(member.state, "state");
      Assert.isNotNull(member.uri, "uri");
      return member;
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).member.equals(member);
    }

    @Override
    public int hashCode() {
      return Objects.hash(member);
    }

    @Override
    public String toString() {
      return String.format("%s[member=%s]", getClass().getCanonicalName(), member);
    }

  }

}
