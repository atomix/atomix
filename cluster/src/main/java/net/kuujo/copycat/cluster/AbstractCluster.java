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
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
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
  protected final Set<EventListener<MembershipChangeEvent>> membershipListeners = new CopyOnWriteArraySet<>();
  protected final Serializer serializer;
  private final AtomicInteger permits = new AtomicInteger();
  private MembershipDetector membershipDetector;
  private CompletableFuture<Cluster> openFuture;
  private CompletableFuture<Void> closeFuture;
  private AtomicBoolean open = new AtomicBoolean();

  protected AbstractCluster(AbstractLocalMember localMember, Collection<? extends AbstractRemoteMember> remoteMembers, Serializer serializer) {
    this.localMember = localMember;
    remoteMembers.forEach(m -> this.remoteMembers.put(m.id(), m));
    this.members.putAll(this.remoteMembers);
    this.members.put(localMember.id(), localMember);
    this.serializer = serializer;
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
            membershipDetector = new MembershipDetector(this, new ExecutionContext(String.format("copycat-membership-detector-%d", localMember.id())));
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

  /**
   * Returns a boolean value indicating whether the cluster is open.
   *
   * @return Indicates whether the cluster is open.
   */
  public boolean isOpen() {
    return permits.get() > 0 && open.get();
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
                closeFuture = null;
                if (permits.get() == 0) {
                  open.set(false);
                }
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
    return !isOpen();
  }

  /**
   * Cluster builder.
   */
  public static abstract class Builder<BUILDER extends Builder<BUILDER, MEMBER>, MEMBER extends ManagedMember> implements Cluster.Builder<BUILDER, MEMBER> {
    protected int memberId;
    protected Member.Type memberType;
    protected final Map<Integer, MEMBER> members = new HashMap<>();
    protected Serializer serializer;

    @Override
    @SuppressWarnings("unchecked")
    public BUILDER withMemberId(int id) {
      if (id < 0)
        throw new IllegalArgumentException("member ID cannot be negative");
      this.memberId = id;
      return (BUILDER) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BUILDER withMemberType(Member.Type type) {
      this.memberType = type;
      return (BUILDER) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BUILDER withSeeds(Collection<MEMBER> members) {
      this.members.clear();
      members.forEach(m -> this.members.put(m.id(), m));
      return (BUILDER) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BUILDER addSeed(MEMBER member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      members.put(member.id(), member);
      return (BUILDER) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BUILDER withSerializer(Serializer serializer) {
      this.serializer = serializer;
      return (BUILDER) this;
    }
  }

}
