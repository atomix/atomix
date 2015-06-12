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
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Managed members.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ManagedMembers implements Members, Managed<Members> {
  protected final Map<Integer, ManagedMember> members = new ConcurrentHashMap<>();
  private List<Member> sortedMembers = new ArrayList<>();
  protected final Set<MembershipListener> membershipListeners = new CopyOnWriteArraySet<>();
  protected final Serializer serializer;
  private final AtomicInteger permits = new AtomicInteger();
  private CompletableFuture<Members> openFuture;
  private CompletableFuture<Void> closeFuture;
  private AtomicBoolean open = new AtomicBoolean();

  protected ManagedMembers(Collection<? extends ManagedMember> remoteMembers, Serializer serializer) {
    remoteMembers.forEach(m -> {
      ((ManagedMember)m).setContext(new ExecutionContext("copycat-cluster-" + m.id(), serializer));
      this.members.put(m.id(), m);
      this.sortedMembers.add(m);
    });
    Collections.sort(sortedMembers, (m1, m2) -> m1.id() - m2.id());
    this.serializer = serializer;
  }

  /**
   * Configures the set of active cluster members.
   */
  public CompletableFuture<Void> configure(MemberInfo... membersInfo) {
    List<CompletableFuture<?>> futures = new ArrayList<>();
    for (MemberInfo memberInfo : membersInfo) {
      if (!members.containsKey(memberInfo.id())) {
        ManagedRemoteMember member = createMember(memberInfo);
        futures.add(member.open().thenRun(() -> {
          member.type = Member.Type.PASSIVE;
          members.put(member.id(), member);
          membershipListeners.forEach(l -> l.memberJoined(member));
        }));
      }
    }

    for (ManagedMember member : members.values()) {
      if (member.type() == Member.Type.PASSIVE) {
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
            membershipListeners.forEach(l -> l.memberLeft(member.id()));
          }));
        }
      }
    }

    sortedMembers = new ArrayList<>(members.values());
    Collections.sort(sortedMembers, (m1, m2) -> m1.id() - m2.id());

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  /**
   * Adds a member to the cluster.
   */
  public synchronized CompletableFuture<Void> addMember(MemberInfo memberInfo) {
    if (!members.containsKey(memberInfo.id())) {
      ManagedRemoteMember member = createMember(memberInfo);
      return member.open().thenRun(() -> {
        member.type = Member.Type.PASSIVE;
        members.put(member.id(), member);
        membershipListeners.forEach(l -> l.memberJoined(member));
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Removes a member from the cluster.
   */
  public synchronized CompletableFuture<Void> removeMember(int memberId) {
    if (members.containsKey(memberId)) {
      ManagedMember member = members.remove(memberId);
      return member.close().whenComplete((result, error) -> {
        membershipListeners.forEach(l -> l.memberLeft(memberId));
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Configures the type of a member.
   */
  public synchronized CompletableFuture<Void> configureMember(int memberId, Member.Type type) {
    ManagedMember member = members.get(memberId);
    if (member != null) {
      member.type = type;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Configures the status of a member.
   */
  public synchronized CompletableFuture<Void> configureMember(int memberId, Member.Status status) {
    ManagedMember member = members.get(memberId);
    if (member != null) {
      member.status = status;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Creates a new remote member.
   */
  protected abstract ManagedRemoteMember createMember(MemberInfo info);

  @Override
  public ManagedMember member(int id) {
    ManagedMember member = members.get(id);
    if (member == null)
      throw new NoSuchElementException();
    return member;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Member> members() {
    return sortedMembers;
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public Members addListener(MembershipListener listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.add(listener);
    return this;
  }

  @Override
  public Members removeListener(MembershipListener listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    membershipListeners.remove(listener);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Members> open() {
    if (permits.incrementAndGet() == 1) {
      synchronized (this) {
        if (openFuture == null) {
          int i = 0;
          CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size()];
          for (ManagedMember member : members.values()) {
            futures[i++] = member.open();
          }
          openFuture = CompletableFuture.allOf(futures).thenApply(v -> {
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
          CompletableFuture<Void>[] futures = new CompletableFuture[members.size()];
          for (ManagedMember member : members.values()) {
            futures[i++] = member.close();
          }
          closeFuture = CompletableFuture.allOf(futures)
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
  public static abstract class Builder<T extends Builder<T, U>, U extends ManagedMember> implements Members.Builder<T, ManagedMembers, U> {
    protected final Map<Integer, U> members = new HashMap<>();

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
