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
package net.kuujo.copycat.raft.server.log;

import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.storage.Entry;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Configuration entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigurationEntry extends RaftEntry<ConfigurationEntry> {
  private Members active;
  private Members passive;

  public ConfigurationEntry() {
  }

  public ConfigurationEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the active members.
   *
   * @return The active members.
   */
  public Members getActive() {
    return active;
  }

  /**
   * Sets the active members.
   *
   * @param members The active members.
   * @return The configuration entry.
   */
  public ConfigurationEntry setActive(Members members) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    this.active = members;
    return this;
  }

  /**
   * Returns the passive members.
   *
   * @return The passive members.
   */
  public Members getPassive() {
    return passive;
  }

  /**
   * Sets the passive members.
   *
   * @param members The passive members.
   * @return The configuration entry.
   */
  public ConfigurationEntry setPassive(Members members) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    this.passive = members;
    return this;
  }

  @Override
  public int size() {
    return super.size() + Integer.BYTES;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(active, buffer);
    serializer.writeObject(passive, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    active = serializer.readObject(buffer);
    passive = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, active=%s, passive=%s]", getClass().getSimpleName(), getIndex(), getTerm(), active, passive);
  }

}
