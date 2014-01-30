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
package net.kuujo.mimeo.cluster.config.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import net.kuujo.mimeo.cluster.config.ClusterConfig;

/**
 * A static cluster configuration.
 * 
 * @author Jordan Halterman
 */
public class StaticClusterConfig extends ClusterConfig {
  private Set<String> members = new HashSet<>();
  private boolean locked;

  @Override
  public ClusterConfig setMembers(String... members) {
    checkLock();
    this.members = new HashSet<String>(Arrays.asList(members));
    return this;
  }

  @Override
  public ClusterConfig setMembers(Set<String> members) {
    checkLock();
    this.members = members;
    return this;
  }

  @Override
  public ClusterConfig addMember(String address) {
    checkLock();
    members.add(address);
    return this;
  }

  @Override
  public boolean containsMember(String address) {
    return members.contains(address);
  }

  @Override
  public ClusterConfig removeMember(String address) {
    checkLock();
    members.remove(address);
    return this;
  }

  @Override
  public Set<String> getMembers() {
    return new HashSet<>(members);
  }

  private void checkLock() {
    if (locked) {
      throw new IllegalStateException("Cannot modify static cluster configuration.");
    }
  }

  @Override
  public void lock() {
    locked = true;
  }

}
