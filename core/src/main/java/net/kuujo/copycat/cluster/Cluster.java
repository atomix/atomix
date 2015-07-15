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

import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.Session;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster {
  private final Raft raft;
  private final Map<Integer, Member> members = new ConcurrentHashMap<>();
  private final Map<String, ClusterTopic> topics = new ConcurrentHashMap<>();

  public Cluster(Raft raft) {
    this.raft = raft;
    listen();
  }

  /**
   * Starts listening for new sessions.
   */
  private void listen() {
    raft.sessions().onSession(session -> {
      session.onOpen(this::addMember);
      session.onClose(this::removeMember);
    });
  }

  /**
   * Adds a member when a session is opened.
   */
  private void addMember(Session session) {
    members.put(session.member(), new Member(session));
    session.onReceive(this::receive);
  }

  /**
   * Removes a member when a session is opened.
   */
  private void removeMember(Session session) {
    members.remove(session.member());
  }

  /**
   * Returns a cluster topic.
   *
   * @param name The topic name.
   * @param <T> The topic message type.
   * @return The topic.
   */
  @SuppressWarnings("unchecked")
  public <T> Topic<T> topic(String name) {
    return topics.computeIfAbsent(name, ClusterTopic::new);
  }

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member.
   */
  public Member member(int id) {
    return members.get(id);
  }

  /**
   * Returns an immutable collection of all cluster members.
   *
   * @return A collection of all cluster members.
   */
  public Collection<Member> members() {
    return members.values();
  }

  /**
   * Receives a message.
   */
  private void receive(Object message) {
    receive((Message) message);
  }

  /**
   * Receives a message.
   */
  @SuppressWarnings("unchecked")
  private void receive(Message message) {
    ClusterTopic topic = topics.get(message.topic());
    if (topic != null) {
      topic.invoke(message);
    }
  }

  /**
   * Cluster topic.
   */
  private class ClusterTopic<T> extends Topic<T> {
    private ClusterTopic(String name) {
      super(name);
    }

    @Override
    public CompletableFuture<Void> publish(T message) {
      int i = 0;
      CompletableFuture[] futures = new CompletableFuture[members.size()];
      for (Member member : members.values()) {
        futures[i++] = member.session().publish(new Message<>(name, member.id(), message));
      }
      return CompletableFuture.allOf(futures);
    }

    /**
     * Invokes topic listeners.
     */
    protected void invoke(Message<T> message) {
      for (ListenerContext<Message<T>> listener : listeners) {
        listener.accept(message);
      }
    }

    @Override
    protected void close() {
      topics.remove(name);
    }
  }

}
