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
import net.kuujo.copycat.raft.Session;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Member {
  private final Session session;
  private final Map<String, MemberTopic> topics = new ConcurrentHashMap<>();

  Member(Session session) {
    this.session = session;
    listen();
  }

  /**
   * Returns the member session.
   *
   * @return The member session.
   */
  Session session() {
    return session;
  }

  /**
   * Starts listening on the session.
   */
  private void listen() {
    session.onReceive(this::receive);
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int id() {
    return session.member();
  }

  /**
   * Returns a topic for the member.
   *
   * @param name The topic name.
   * @return The topic.
   */
  @SuppressWarnings("unchecked")
  public <T> Topic<T> topic(String name) {
    return topics.computeIfAbsent(name, MemberTopic::new);
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
    MemberTopic topic = topics.get(message.topic());
    if (topic != null) {
      topic.invoke(message);
    }
  }

  /**
   * Member topic.
   */
  private class MemberTopic<T> extends Topic<T> {
    private MemberTopic(String name) {
      super(name);
    }

    @Override
    public CompletableFuture<Void> publish(T message) {
      return session.publish(new Message<T>(name, session.member(), message));
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
