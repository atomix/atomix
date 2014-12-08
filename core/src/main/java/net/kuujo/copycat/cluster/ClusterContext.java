/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.election.Election;

import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterContext extends Observable {
  private String localMember;
  private Set<String> remoteMembers;
  private String leader;
  private long term;
  private Election.Status status;

  public ClusterContext(ClusterConfig config) {
    this(config.getLocalMember(), config.getRemoteMembers());
  }

  public ClusterContext(String localMember) {
    this(localMember, new HashSet<>());
  }

  public ClusterContext(String localMember, Set<String> remoteMembers) {
    this.localMember = localMember;
    remoteMembers.remove(localMember);
    this.remoteMembers = remoteMembers;
  }

  public String getLocalMember() {
    return localMember;
  }

  public ClusterContext setMembers(Set<String> members) {
    if (!members.contains(localMember)) {
      throw new IllegalStateException("Cluster membership must contain the local member");
    }
    members.remove(localMember);
    this.remoteMembers = members;
    setChanged();
    notifyObservers();
    clearChanged();
    return this;
  }

  public Set<String> getMembers() {
    Set<String> members = new HashSet<>(remoteMembers);
    members.add(localMember);
    return members;
  }

  public Set<String> getRemoteMembers() {
    return remoteMembers;
  }

  public ClusterContext addMember(String uri) {
    if (remoteMembers.add(uri)) {
      setChanged();
      notifyObservers();
      clearChanged();
    }
    return this;
  }

  public ClusterContext removeMember(String uri) {
    if (remoteMembers.remove(uri)) {
      setChanged();
      notifyObservers();
      clearChanged();
    }
    return this;
  }

  public ClusterContext setLeader(String leader) {
    if (this.leader == null) {
      if (leader != null) {
        this.leader = leader;
        this.status = Election.Status.COMPLETE;
        setChanged();
        notifyObservers();
        clearChanged();
      }
    } else if (leader != null) {
      if (!this.leader.equals(leader)) {
        this.leader = leader;
        this.status = Election.Status.COMPLETE;
        setChanged();
        notifyObservers();
        clearChanged();
      }
    } else {
      this.leader = null;
      this.status = Election.Status.IN_PROGRESS;
      setChanged();
      notifyObservers();
      clearChanged();
    }
    return this;
  }

  public String getLeader() {
    return leader;
  }

  public ClusterContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = null;
      this.status = Election.Status.IN_PROGRESS;
      setChanged();
      notifyObservers();
      clearChanged();
    }
    return this;
  }

  public long getTerm() {
    return term;
  }

  public Election.Status getStatus() {
    return status;
  }

  public ClusterConfig getConfig() {
    return new ClusterConfig()
      .withLocalMember(localMember)
      .withRemoteMembers(remoteMembers);
  }

}
