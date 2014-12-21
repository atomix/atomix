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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Cluster membership info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Membership extends MemberInfo {
  private Map<String, MemberInfo> members = new HashMap<>();

  public Membership(String uri, Collection<String> members) {
    super(uri);
    for (String member : members) {
      this.members.put(member, new MemberInfo(member));
    }
  }

  /**
   * Increments the membership version.
   *
   * @return The membership instance.
   */
  public Membership increment() {
    version++;
    return this;
  }

  /**
   * Returns member info for a specific member.
   *
   * @param uri The URI for which to return member info.
   * @return The member info.
   */
  public MemberInfo member(String uri) {
    return members.get(uri);
  }

  /**
   * Returns a collection of cluster member info.
   *
   * @return A collection of cluster member info.
   */
  public Collection<MemberInfo> members() {
    return members.values();
  }

  /**
   * Updates the cluster membership.
   *
   * @param membership The membership with which to update the membership state.
   */
  public void update(Membership membership) {
    // Update the member from which this membership was received.
    members.put(membership.uri, membership);

    // Iterate through member info and update all members.
    for (MemberInfo info : membership.members()) {
      // If the given member doesn't exist or has a version greater than the recorded version then persist the member.
      MemberInfo record = members.get(info.uri());
      if (record == null || info.version() > record.version()) {
        members.put(info.uri(), info);
      }
      // If the given member's last index is greater than the local last index then update the leader and term.
      if (info.index > index) {
        this.leader = info.leader;
        this.term = info.term;
      }
    }

    // Increment the membership version.
    increment();
  }

}
