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
package net.kuujo.copycat.raft.protocol;

import java.io.Serializable;
import java.util.Objects;

/**
 * Raft replica info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReplicaInfo implements Serializable {
  private String uri;
  private long version = 1;
  private Long index;

  public ReplicaInfo() {
  }

  public ReplicaInfo(String uri) {
    this(uri, 1, null);
  }

  public ReplicaInfo(String uri, long version, Long index) {
    this.uri = uri;
    this.version = version;
    this.index = index;
  }

  /**
   * Returns the replica URI.
   *
   * @return The replica URI.
   */
  public String getUri() {
    return uri;
  }

  /**
   * Returns the replica version.
   *
   * @return The replica version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the replica version.
   *
   * @param version The replica version.
   * @return The replica info.
   */
  public ReplicaInfo setVersion(long version) {
    this.version = version;
    return this;
  }

  /**
   * Returns the replica index.
   *
   * @return The replica index.
   */
  public Long getIndex() {
    return index;
  }

  /**
   * Sets the replica index.
   *
   * @param index The replica index.
   * @return The replica info.
   */
  public ReplicaInfo setIndex(Long index) {
    this.index = index;
    return this;
  }

  /**
   * Updates the replica info.
   *
   * @param info The replica info to update.
   */
  public void update(ReplicaInfo info) {
    if (info.version > this.version) {
      this.version = info.version;
      this.index = info.index;
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReplicaInfo) {
      ReplicaInfo replica = (ReplicaInfo) object;
      return replica.uri.equals(uri)
        && replica.version == version
        && replica.index.equals(index);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, version, index);
  }

  @Override
  public String toString() {
    return String.format("MemberInfo[uri=%s, version=%d, index=%s]", uri, version, index);
  }

}
