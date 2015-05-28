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
import net.kuujo.copycat.util.Managed;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Managed cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ManagedCluster implements Cluster, Managed<Cluster> {
  protected final ManagedLocalMember localMember;
  protected final Map<Integer, ManagedRemoteMember> remoteMembers = new ConcurrentHashMap<>();
  protected final Map<Integer, ManagedMember> members = new ConcurrentHashMap<>();
  protected final Set<MembershipListener> membershipListeners = new CopyOnWriteArraySet<>();
  protected final Serializer serializer;
  private final AtomicInteger permits = new AtomicInteger();
  private CompletableFuture<Cluster> openFuture;
  private CompletableFuture<Void> closeFuture;
  private AtomicBoolean open = new AtomicBoolean();

  protected ManagedCluster(ManagedLocalMember localMember, Collection<? extends ManagedRemoteMember> remoteMembers, Serializer serializer) {
    this.localMember = localMember;
    remoteMembers.forEach(m -> this.remoteMembers.put(m.id(), m));
    this.members.putAll(this.remoteMembers);
    this.members.put(localMember.id(), localMember);
    this.serializer = serializer;
  }

  /**
   * Configures the set of active cluster members.
   */
  public CompletableFuture<Void> configure(MemberInfo... membersInfo) {
    List<CompletableFuture> futures = new ArrayList<>();
    for (MemberInfo memberInfo : membersInfo) {
      if (memberInfo.id() != localMember.id() && !remoteMembers.containsKey(memberInfo.id())) {
        ManagedRemoteMember member = createMember(memberInfo);
        futures.add(member.connect().thenRun(() -> {
          member.type = Member.Type.CLIENT;
          members.put(member.id(), member);
          remoteMembers.put(member.id(), member);
          membershipListeners.forEach(l -> l.memberJoined(member));
        }));
      }
    }

    for (ManagedRemoteMember member : remoteMembers.values()) {
      if (member.type() == Member.Type.CLIENT) {
        boolean configured = false;
        for (MemberInfo memberInfo : membersInfo) {
          if (memberInfo.id() == member.id()) {
            configured = true;
            break;
          }
        }

        if (!configured) {
          futures.add(member.close().thenRun(() -> {
            members.remove(member.id());
            remoteMembers.remove(member.id());
            membershipListeners.forEach(l -> l.memberLeft(member.id()));
          }));
        }
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  /**
   * Creates a new remote member.
   */
  protected abstract ManagedRemoteMember createMember(MemberInfo info);

  @Override
  public ManagedLocalMember member() {
    return localMember;
  }

  @Override
  public Member member(int id) {
    if (localMember.id() == id)
      return localMember;
    ManagedMember member = remoteMembers.get(id);
    if (member == null)
      throw new NoSuchElementException();
    return member;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Member> members() {
    return (Collection) members.values();
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public <T> Cluster broadcast(String topic, T message) {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
    remoteMembers.values().forEach(m -> {
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

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Cluster> open() {
    if (permits.incrementAndGet() == 1) {
      synchronized (this) {
        if (openFuture == null) {
          openFuture = localMember.listen().thenCompose(v -> {
            int i = 0;
            CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size() - 1];
            for (ManagedRemoteMember member : remoteMembers.values()) {
              futures[i++] = member.connect();
            }
            return CompletableFuture.allOf(futures);
          }).thenApply(v -> {
            openFuture = null;
            if (permits.get() > 0) {
              open.set(true);
            }
            return this;
          });
        }
      }
      return openFuture;
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return permits.get() > 0 && open.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    if (permits.decrementAndGet() == 0) {
      synchronized (this) {
        if (closeFuture == null) {
          int i = 0;
          CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size()-1];
          for (ManagedRemoteMember member : remoteMembers.values()) {
            futures[i++] = member.connect();
          }
          closeFuture = CompletableFuture.allOf(futures)
            .thenCompose(v -> localMember.close())
            .thenRun(() -> {
              closeFuture = null;
              if (permits.get() == 0) {
                open.set(false);
              }
            });
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return !isOpen();
  }

  /**
   * Cluster builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ManagedMember> implements Cluster.Builder<T, ManagedCluster, U> {
    protected int memberId;
    protected Member.Type type = Member.Type.CLIENT;
    protected final Map<Integer, U> members = new HashMap<>();
    protected Serializer serializer;

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
    public T withMemberType(Member.Type type) {
      if (type == null)
        throw new NullPointerException("type cannot be null");
      this.type = type;
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

    @Override
    @SuppressWarnings("unchecked")
    public T withSerializer(Serializer serializer) {
      this.serializer = serializer;
      return (T) this;
    }
  }

}
