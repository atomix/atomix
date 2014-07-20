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
package net.kuujo.copycat.cluster.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.ProtocolUri;

/**
 * Static cluster configuration that does not change once the cluster
 * has been started.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StaticClusterConfig implements ClusterConfig {
  private int quorumSize;
  private String localMember;
  private final Set<String> remoteMembers = new HashSet<>();

  public StaticClusterConfig() {
    this(null);
  }

  public StaticClusterConfig(String local) {
    this.localMember = local;
  }

  @Override
  public int getQuorumSize() {
    return (int) (quorumSize > 0 ? quorumSize : Math.floor((remoteMembers.size() + 1) / 2) + 1);
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
  public ClusterConfig setLocalMember(String uri) {
    if (!ProtocolUri.isValidUri(uri)) {
      throw new IllegalArgumentException(uri + " is not a valid protocol URI");
    }
    this.localMember = uri;
    return this;
  }

  @Override
  public Set<String> getRemoteMembers() {
    return remoteMembers;
  }

  @Override
  public ClusterConfig setRemoteMembers(String... members) {
    remoteMembers.clear();
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    return this;
  }

  @Override
  public ClusterConfig setRemoteMembers(Set<String> members) {
    remoteMembers.clear();
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    return this;
  }

  @Override
  public ClusterConfig addRemoteMember(String uri) {
    if (!ProtocolUri.isValidUri(uri)) {
      throw new IllegalArgumentException(uri + " is not a valid protocol URI");
    }
    remoteMembers.add(uri);
    return this;
  }

  @Override
  public ClusterConfig addRemoteMembers(String... members) {
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    return this;
  }

  @Override
  public ClusterConfig addRemoteMembers(Set<String> members) {
    for (String uri : members) {
      if (!ProtocolUri.isValidUri(uri)) {
        throw new IllegalArgumentException(uri + " is not a valid protocol URI");
      }
      remoteMembers.add(uri);
    }
    return this;
  }

  @Override
  public ClusterConfig removeRemoteMember(String uri) {
    remoteMembers.remove(uri);
    return this;
  }

  @Override
  public ClusterConfig removeRemoteMembers(String... members) {
    remoteMembers.removeAll(Arrays.asList(members));
    return this;
  }

  @Override
  public ClusterConfig removeRemoteMembers(Set<String> members) {
    remoteMembers.removeAll(members);
    return this;
  }

}
