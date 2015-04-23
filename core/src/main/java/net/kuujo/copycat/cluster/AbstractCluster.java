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

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.io.serializer.CopycatSerializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract cluster implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCluster implements ManagedCluster {
  protected final AbstractLocalMember localMember;
  protected final Map<Integer, AbstractRemoteMember> remoteMembers = new ConcurrentHashMap<>();
  protected final Map<Integer, AbstractMember> members = new ConcurrentHashMap<>();
  protected final CopycatSerializer serializer = new CopycatSerializer();
  protected final Set<EventListener<MembershipChangeEvent>> membershipListeners = new CopyOnWriteArraySet<>();
  private final AtomicInteger permits = new AtomicInteger();
  private MembershipDetector membershipDetector;
  private CompletableFuture<Cluster> openFuture;
  private CompletableFuture<Void> closeFuture;

  protected AbstractCluster(AbstractLocalMember localMember, Collection<? extends AbstractRemoteMember> remoteMembers) {
    this.localMember = localMember;
    remoteMembers.forEach(m -> this.remoteMembers.put(m.id(), m));
    this.members.putAll(this.remoteMembers);
    this.members.put(localMember.id(), localMember);
  }

  /**
   * Creates a new remote member.
   */
  protected abstract AbstractRemoteMember createRemoteMember(AbstractMember.Info info);

  @Override
  public LocalMember member() {
    return localMember;
  }

  @Override
  public Member member(int id) {
    if (localMember.id() == id)
      return localMember;
    Member member = remoteMembers.get(id);
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
  public <T> Cluster broadcast(String topic, T message) {
    if (!isOpen())
      throw new IllegalStateException("cluster not open");
    remoteMembers.values().forEach(m -> {
      m.send(topic, message);
    });
    return this;
  }

  @Override
  public Cluster addMembershipListener(EventListener<MembershipChangeEvent> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.add(listener);
    return this;
  }

  @Override
  public Cluster removeMembershipListener(EventListener<MembershipChangeEvent> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.remove(listener);
    return this;
  }

  /**
   * Opens the cluster.
   *
   * @return A completable future to be completed once the cluster has been opened.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Cluster> open() {
    if (permits.incrementAndGet() == 1) {
      synchronized (this) {
        if (openFuture == null) {
          openFuture = localMember.listen().thenCompose(v -> {
            int i = 0;
            CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size() - 1];
            for (AbstractRemoteMember member : remoteMembers.values()) {
              futures[i++] = member.connect();
            }
            return CompletableFuture.allOf(futures);
          }).thenApply(v -> {
            membershipDetector = new MembershipDetector(this);
            return this;
          });
        }
      }
      return openFuture;
    }
    return CompletableFuture.completedFuture(this);
  }

  /**
   * Returns a boolean value indicating whether the cluster is open.
   *
   * @return Indicates whether the cluster is open.
   */
  public boolean isOpen() {
    return permits.get() > 0 && openFuture == null;
  }

  /**
   * Closes the cluster.
   *
   * @return A completable future to be completed once the cluster has been closed.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    if (permits.decrementAndGet() == 0) {
      synchronized (this) {
        if (closeFuture == null) {
          int i = 0;
          CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size()-1];
          for (AbstractRemoteMember member : remoteMembers.values()) {
            futures[i++] = member.connect();
          }
          closeFuture = CompletableFuture.allOf(futures)
            .thenCompose(v -> localMember.close())
            .thenRun(() -> {
              if (membershipDetector != null) {
                membershipDetector.close();
                membershipDetector = null;
              }
            });
        }
      }
    }
    return closeFuture;
  }

  /**
   * Returns a boolean value indicating whether the cluster is closed.
   *
   * @return Indicates whether the cluster is closed.
   */
  public boolean isClosed() {
    return permits.get() == 0 && closeFuture == null;
  }

  /**
   * Cluster builder.
   */
  public static abstract class Builder<L extends ManagedLocalMember, R extends ManagedRemoteMember> implements Cluster.Builder<L, R> {
    protected L localMember;
    protected Collection<R> remoteMembers = new HashSet<>();

    @Override
    public Builder withLocalMember(L member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      localMember = member;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Builder withRemoteMembers(Collection<R> members) {
      remoteMembers.clear();
      remoteMembers.addAll(members);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Builder addRemoteMembers(Collection<R> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      remoteMembers.addAll(members);
      return this;
    }

    @Override
    public Builder withMembers(Collection<Member> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      remoteMembers.clear();
      return addMembers(members);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Builder addMembers(Collection<Member> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      members.forEach(m -> {
        if (m instanceof LocalMember) {
          withLocalMember((L) m);
        } else if (m instanceof RemoteMember) {
          addRemoteMember((R) m);
        }
      });
      return this;
    }
  }

}
