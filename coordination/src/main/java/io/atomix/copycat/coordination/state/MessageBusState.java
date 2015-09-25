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
package io.atomix.copycat.coordination.state;

import io.atomix.catalog.client.session.Session;
import io.atomix.catalog.server.Commit;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.StateMachineExecutor;
import io.atomix.catalyst.transport.Address;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Message bus state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MessageBusState extends StateMachine {
  private final Map<Long, Commit<MessageBusCommands.Join>> members = new HashMap<>();
  private final Map<String, Map<Long, Commit<MessageBusCommands.Register>>> topics = new HashMap<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(MessageBusCommands.Join.class, this::join);
    executor.register(MessageBusCommands.Leave.class, this::leave);
    executor.register(MessageBusCommands.Register.class, (Function<Commit<MessageBusCommands.Register>, CompletableFuture>) this::registerConsumer);
    executor.register(MessageBusCommands.Unregister.class, this::unregisterConsumer);
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
  protected Map<String, Set<Address>> join(Commit<MessageBusCommands.Join> commit) {
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
      commit.clean();
      throw e;
    }
  }

  /**
   * Applies leave commits.
   */
  protected void leave(Commit<MessageBusCommands.Leave> commit) {
    try {
      Commit<MessageBusCommands.Join> previous = members.remove(commit.session().id());
      if (previous != null) {
        previous.clean();

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
      commit.clean();
    }
  }

  /**
   * Registers a topic consumer.
   */
  private CompletableFuture registerConsumer(Commit<MessageBusCommands.Register> commit) {
    try {
      Commit<MessageBusCommands.Join> parent = members.get(commit.session().id());
      if (parent == null) {
        throw new IllegalStateException("unknown session: " + commit.session().id());
      }

      Map<Long, Commit<MessageBusCommands.Register>> registrations = topics.computeIfAbsent(commit.operation().topic(), t -> new HashMap<>());
      registrations.put(commit.session().id(), commit);

      int i = 0;
      CompletableFuture[] futures = new CompletableFuture[members.size()];
      for (Commit<MessageBusCommands.Join> member : members.values()) {
        futures[i++] = member.session().publish("register", new MessageBusCommands.ConsumerInfo(commit.operation().topic(), parent.operation().member()));
      }
      return CompletableFuture.allOf(futures);
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

  /**
   * Unregisters a topic consumer.
   */
  private void unregisterConsumer(Commit<MessageBusCommands.Unregister> commit) {
    try {
      Map<Long, Commit<MessageBusCommands.Register>> registrations = topics.get(commit.operation().topic());
      if (registrations != null) {
        Commit<MessageBusCommands.Register> registration = registrations.remove(commit.session().id());
        if (registration != null) {
          registration.clean();

          Commit<MessageBusCommands.Join> parent = members.get(registration.session().id());
          if (parent != null) {
            for (Commit<MessageBusCommands.Join> member : members.values()) {
              member.session().publish("unregister", new MessageBusCommands.ConsumerInfo(commit.operation().topic(), parent.operation().member()));
            }
          }
        }
      }
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

}
