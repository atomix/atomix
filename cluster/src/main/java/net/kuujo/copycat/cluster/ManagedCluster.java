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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.io.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;

/**
 * Managed cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ManagedCluster extends ManagedMembers implements Cluster {
  protected final ManagedLocalMember localMember;
  protected final Set<MembershipListener> membershipListeners = new CopyOnWriteArraySet<>();

  protected ManagedCluster(ManagedLocalMember localMember, Collection<? extends ManagedRemoteMember> remoteMembers, Serializer serializer) {
    super(((Supplier<Collection<ManagedMember>>) () -> {
      Collection<ManagedMember> members = new ArrayList<>(remoteMembers);
      members.add(localMember);
      return members;
    }).get(), serializer);
    this.localMember = localMember;
  }

  @Override
  public ManagedLocalMember member() {
    return localMember;
  }

  @Override
  public <T> Cluster broadcast(T message) {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
    members.values().forEach(m -> {
      m.send(message);
    });
    return this;
  }

  @Override
  public <T> Cluster broadcast(Class<? super T> type, T message) {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
    members.values().forEach(m -> {
      m.send(type, message);
    });
    return this;
  }

  @Override
  public <T> Cluster broadcast(String topic, T message) {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
    members.values().forEach(m -> {
      m.send(topic, message);
    });
    return this;
  }

  @Override
  public Cluster addListener(MembershipListener listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.add(listener);
    return this;
  }

  @Override
  public Cluster removeListener(MembershipListener listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.remove(listener);
    return this;
  }

  /**
   * Cluster builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ManagedMember> implements Cluster.Builder<T, ManagedCluster, U> {
    protected int memberId;
    protected final Map<Integer, U> members = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public T withMemberId(int id) {
      if (id < 0)
        throw new IllegalArgumentException("member ID cannot be negative");
      this.memberId = id;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withMembers(Collection<U> members) {
      this.members.clear();
      members.forEach(m -> this.members.put(m.id(), m));
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T addMember(U member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      members.put(member.id(), member);
      return (T) this;
    }
  }

}
