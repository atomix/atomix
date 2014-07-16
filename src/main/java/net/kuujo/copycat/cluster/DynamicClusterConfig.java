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
package net.kuujo.copycat.cluster;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

/**
 * Cluster configuration implementation that can be arbitrarily updated
 * by the user during runtime.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DynamicClusterConfig extends Observable implements ClusterConfig {
  private int quorumSize;
  private String localMember;
  private final Set<String> remoteMembers = new HashSet<>();

  public DynamicClusterConfig() {
    this(null, 0);
  }

  public DynamicClusterConfig(String local) {
    this(local, 0);
  }

  public DynamicClusterConfig(String local, int quorumSize) {
    this.localMember = local;
    this.quorumSize = quorumSize;
  }

  @Override
  public int getQuorumSize() {
    return (int) (quorumSize > 0 ? quorumSize : Math.floor((remoteMembers.size() + 1) / 2) + 1);
  }

  @Override
  public ClusterConfig setQuorumSize(int quorumSize) {
    this.quorumSize = quorumSize;
    return this;
  }

  @Override
  public Set<String> getMembers() {
    Set<String> members = new HashSet<>(remoteMembers);
    if (localMember != null) {
      members.add(localMember);
    }
    return members;
  }

  @Override
  public String getLocalMember() {
    return localMember;
  }

  @Override
  public ClusterConfig setLocalMember(String address) {
    this.localMember = address;
    callObservers();
    return this;
  }

  @Override
  public Set<String> getRemoteMembers() {
    return remoteMembers;
  }

  @Override
  public ClusterConfig setRemoteMembers(String... members) {
    remoteMembers.clear();
    remoteMembers.addAll(Arrays.asList(members));
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig setRemoteMembers(Set<String> members) {
    remoteMembers.clear();
    remoteMembers.addAll(members);
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig addRemoteMember(String address) {
    remoteMembers.add(address);
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig addRemoteMembers(String... members) {
    remoteMembers.addAll(Arrays.asList(members));
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig addRemoteMembers(Set<String> members) {
    remoteMembers.addAll(members);
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig removeRemoteMember(String address) {
    remoteMembers.remove(address);
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig removeRemoteMembers(String... members) {
    remoteMembers.removeAll(Arrays.asList(members));
    callObservers();
    return this;
  }

  @Override
  public ClusterConfig removeRemoteMembers(Set<String> members) {
    remoteMembers.removeAll(members);
    callObservers();
    return this;
  }

  private void callObservers() {
    setChanged();
    notifyObservers();
    clearChanged();
  }

}
