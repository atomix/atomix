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

import java.io.Serializable;

/**
 * Cluster member info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemberInfo implements Serializable {
  public static final int STATUS_ACTIVE = 1;
  public static final int STATUS_INACTIVE = 0;

  protected String uri;
  protected long version = 1;
  protected Long index;
  protected String leader;
  protected long term;
  protected int status = STATUS_ACTIVE;

  public MemberInfo(String uri) {
    this(uri, 1, null, null, 1, 1);
  }

  public MemberInfo(String uri, long version, Long index, String leader, long term, int status) {
    this.uri = uri;
    this.version = version;
    this.index = index;
    this.leader = leader;
    this.term = term;
    this.status = status;
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
   * Returns the currently known cluster leader.
   *
   * @return The currently known cluster leader.
   */
  public String leader() {
    return leader;
  }

  /**
   * Returns the currently known cluster term.
   *
   * @return The currently known cluster term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the member status.
   *
   * @return The member status.
   */
  public int status() {
    return status;
  }

  /**
   * Updates the member info.
   *
   * @param info The member info to update.
   */
  public void update(MemberInfo info) {
    this.version = info.version;
    this.index = info.index;
    this.leader = info.leader;
    this.term = info.term;
    this.status = info.status;
  }

}
