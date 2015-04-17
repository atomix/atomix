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
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.resource.PartitionContext;
import net.kuujo.copycat.util.Managed;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Managed members.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ManagedMembers implements Members, Managed<Void>, Observer {
  private final ClusterConfig config;
  private final Protocol protocol;
  private final PartitionContext context;
  private final RaftContext raft;
  @SuppressWarnings("rawtypes")
  final Map<Integer, ManagedMember> members = new ConcurrentHashMap<>(128);
  private final Set<EventListener<MembershipEvent>> listeners = new CopyOnWriteArraySet<>();
  private boolean open;

  ManagedMembers(ClusterConfig config, PartitionContext context) {
    if (config == null)
      throw new NullPointerException("config cannot be null");
    if (context == null)
      throw new NullPointerException("context cannot be null");
    this.config = config;
    this.context = context;
    this.protocol = config.getProtocol();
    this.raft = context.getContext();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void update(Observable o, Object arg) {
    RaftContext raft = (RaftContext) o;
    Set<Integer> ids = raft.getMembers().stream().map(RaftMember::id).collect(Collectors.toSet());
    Iterator<Map.Entry<Integer, ManagedMember>> iterator = members.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Integer, ManagedMember> entry = iterator.next();
      if (!ids.contains(entry.getValue().id())) {
        listeners.forEach(l -> l.accept(new MembershipEvent(MembershipEvent.Type.LEAVE, entry.getValue())));
        entry.getValue().close().join();
        iterator.remove();
      }
    }

    for (RaftMember member : raft.getMembers()) {
      if (!members.containsKey(member.id())) {
        try {
          members.put(member.id(), (ManagedMember) new ManagedRemoteMember(member.id(), member.get("address"), protocol, context).open().get());
          listeners.forEach(l -> l.accept(new MembershipEvent(MembershipEvent.Type.JOIN, members.get(member.id()))));
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public Members addListener(EventListener<MembershipEvent> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    listeners.add(listener);
    return this;
  }

  @Override
  public Members removeListener(EventListener<MembershipEvent> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    listeners.remove(listener);
    return this;
  }

  @Override
  public int size() {
    return members.size();
  }

  @Override
  public boolean isEmpty() {
    return members.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return members.containsKey(o);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Member> iterator() {
    return (Iterator<Member>) ((Iterable<? extends Member>) members.values()).iterator();
  }

  @Override
  public Object[] toArray() {
    return members.values().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return members.values().toArray(a);
  }

  @Override
  public boolean add(Member member) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return members.keySet().containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Member> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public CompletableFuture<Void> open() {
    if (open) {
      return CompletableFuture.completedFuture(null);
    }

    open = true;
    raft.addObserver(this);
    members.clear();

    ManagedLocalMember localMember = new ManagedLocalMember(config.getLocalMember().getId(), config.getLocalMember().getAddress(), protocol, context);
    return localMember.open().thenCompose(v -> {
      members.put(localMember.id(), localMember);
      if (context.getPartitionConfig().getReplicas().isEmpty()) {
        for (MemberConfig member : config.getMembers()) {
          if (member.getId() != config.getLocalMember().getId()) {
            members.put(member.getId(), new ManagedRemoteMember(member.getId(), member.getAddress(), protocol, context));
          }
        }
      } else {
        for (int replica : context.getPartitionConfig().getReplicas()) {
          if (replica != config.getLocalMember().getId() && config.hasMember(replica)) {
            members.put(config.getMember(replica).getId(), new ManagedRemoteMember(replica, config.getMember(replica).getAddress(), protocol, context));
          }
        }
      }

      CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size()];
      int i = 0;
      for (ManagedMember member : members.values()) {
        futures[i++] = member.open();
      }
      return CompletableFuture.allOf(futures);
    });
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public CompletableFuture<Void> close() {
    raft.deleteObserver(this);

    if (!open) {
      return CompletableFuture.completedFuture(null);
    }

    open = false;
    CompletableFuture<? extends Member>[] futures = new CompletableFuture[members.size()];
    int i = 0;
    for (ManagedMember member : members.values()) {
      futures[i++] = member.close();
    }
    members.clear();
    return CompletableFuture.allOf(futures);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
