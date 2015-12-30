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
package io.atomix.messaging.state;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;

import java.util.*;

/**
 * Message bus state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MessageBusState extends ResourceStateMachine implements SessionListener {
  private final Map<Long, Commit<MessageBusCommands.Join>> members = new HashMap<>();
  private final Map<String, Map<Long, Commit<MessageBusCommands.Register>>> topics = new HashMap<>();

  @Override
  public void register(Session session) {

  }

  @Override
  public void unregister(Session session) {

  }

  @Override
  public void expire(Session session) {

  }

  @Override
  public void close(Session session) {
    members.remove(session.id());
    for (Commit<MessageBusCommands.Join> member : members.values()) {
      member.session().publish("leave", session.id());
    }
  }

  /**
   * Applies join commits.
   */
  public Map<String, Set<Address>> join(Commit<MessageBusCommands.Join> commit) {
    try {
      members.put(commit.session().id(), commit);

      Map<String, Set<Address>> topics = new HashMap<>();
      for (Map.Entry<String, Map<Long, Commit<MessageBusCommands.Register>>> entry : this.topics.entrySet()) {
        Set<Address> addresses = new HashSet<>();
        for (Map.Entry<Long, Commit<MessageBusCommands.Register>> subEntry : entry.getValue().entrySet()) {
          Commit<MessageBusCommands.Join> member = members.get(subEntry.getKey());
          if (member != null) {
            addresses.add(member.operation().member());
          }
        }
        topics.put(entry.getKey(), addresses);
      }
      return topics;
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Applies leave commits.
   */
  public void leave(Commit<MessageBusCommands.Leave> commit) {
    try {
      Commit<MessageBusCommands.Join> previous = members.remove(commit.session().id());
      if (previous != null) {
        previous.close();

        Iterator<Map.Entry<String, Map<Long, Commit<MessageBusCommands.Register>>>> iterator = topics.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<String, Map<Long, Commit<MessageBusCommands.Register>>> entry = iterator.next();
          String topic = entry.getKey();
          Map<Long, Commit<MessageBusCommands.Register>> registrations = entry.getValue();

          Commit<MessageBusCommands.Register> registration = registrations.remove(commit.session().id());
          if (registration != null) {
            for (Commit<MessageBusCommands.Join> member : members.values()) {
              member.session().publish("unregister", new MessageBusCommands.ConsumerInfo(topic, previous.operation().member()));
            }

            if (registrations.isEmpty()) {
              iterator.remove();
            }
          }
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Registers a topic consumer.
   */
  public void registerConsumer(Commit<MessageBusCommands.Register> commit) {
    try {
      Commit<MessageBusCommands.Join> parent = members.get(commit.session().id());
      if (parent == null) {
        throw new IllegalStateException("unknown session: " + commit.session().id());
      }

      Map<Long, Commit<MessageBusCommands.Register>> registrations = topics.computeIfAbsent(commit.operation().topic(), t -> new HashMap<>());
      registrations.put(commit.session().id(), commit);

      for (Commit<MessageBusCommands.Join> member : members.values()) {
        member.session().publish("register", new MessageBusCommands.ConsumerInfo(commit.operation().topic(), parent.operation().member()));
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Unregisters a topic consumer.
   */
  public void unregisterConsumer(Commit<MessageBusCommands.Unregister> commit) {
    try {
      Map<Long, Commit<MessageBusCommands.Register>> registrations = topics.get(commit.operation().topic());
      if (registrations != null) {
        Commit<MessageBusCommands.Register> registration = registrations.remove(commit.session().id());
        if (registration != null) {
          registration.close();

          Commit<MessageBusCommands.Join> parent = members.get(registration.session().id());
          if (parent != null) {
            for (Commit<MessageBusCommands.Join> member : members.values()) {
              member.session().publish("unregister", new MessageBusCommands.ConsumerInfo(commit.operation().topic(), parent.operation().member()));
            }
          }
        }
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  @Override
  public void delete() {
    members.values().forEach(Commit::close);
    topics.values().forEach(m -> m.values().forEach(Commit::close));
    topics.clear();
  }

}
