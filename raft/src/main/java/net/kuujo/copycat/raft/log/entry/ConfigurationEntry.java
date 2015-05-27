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
package net.kuujo.copycat.raft.log.entry;

import net.kuujo.copycat.cluster.MemberInfo;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.util.Set;

/**
 * Cluster configuration entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=1001)
public class ConfigurationEntry extends RaftEntry<ConfigurationEntry> {
  private long sessionTimeout;
  private Set<MemberInfo> members;

  public ConfigurationEntry() {
  }

  public ConfigurationEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout.
   * @return The configuration entry.
   */
  public ConfigurationEntry setSessionTimeout(long sessionTimeout) {
    if (sessionTimeout <= 0)
      throw new IllegalArgumentException("session timeout must be positive");
    this.sessionTimeout = sessionTimeout;
    return this;
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Set<MemberInfo> getMembers() {
    return members;
  }

  /**
   * Sets the cluster members.
   *
   * @param members The cluster members.
   * @return The configuration entry.
   */
  public ConfigurationEntry setMembers(Set<MemberInfo> members) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    this.members = members;
    return this;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(sessionTimeout);
    serializer.writeObject(members, buffer);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    sessionTimeout = buffer.readLong();
    members = serializer.readObject(buffer);
  }

}
