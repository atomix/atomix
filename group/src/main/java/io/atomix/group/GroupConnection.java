/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.Futures;

import java.util.concurrent.CompletableFuture;

/**
 * Facilitates direct communication between group members.
 * <p>
 * Members of a group and group instances can communicate with one another through the direct messaging API,
 * {@link GroupConnection}. Direct messaging between group members is considered <em>unreliable</em> and is
 * done over the local node's configured {@link io.atomix.catalyst.transport.Transport}. Messages between members
 * of a group are ordered according only to the transport and are not guaranteed to be delivered. While request-reply
 * can be used to achieve some level of assurance that messages are delivered to specific members of the group,
 * direct message consumers should be idempotent and commutative.
 * <p>
 * In order to enable direct messaging for a group instance, the instance must be initialized with
 * {@link DistributedGroup.Options} that define the {@link Address} to which to bind a {@link Server} for messaging.
 * <pre>
 *   {@code
 *   DistributedGroup.Options options = new DistributedGroup.Options()
 *     .withAddress(new Address("localhost", 6000));
 *   DistributedGroup group = atomix.getGroup("message-group", options).get();
 *   }
 * </pre>
 * Once a group instance has been configured with an address for direct messaging, messages can be sent between
 * group members using the {@link GroupConnection} for any member of the group. Messages sent between members must
 * be associated with a {@link String} topic, and messages can be any value that is serializable by the group instance's
 * {@link io.atomix.catalyst.serializer.Serializer}.
 * <pre>
 *   {@code
 *   group.member("foo").connection().send("hello", "World!").thenAccept(reply -> {
 *     ...
 *   });
 *   }
 * </pre>
 * Direct messages can only be <em>received</em> by a {@link LocalGroupMember} which must be created by
 * joining the group. Local members register a listener for a link topic on the joined member's
 * {@link LocalGroupConnection}. Message listeners are asynchronous. When a {@link GroupMessage} is received
 * by a local member, the member can perform any processing it wishes and {@link GroupMessage#reply(Object) reply}
 * to the message or {@link GroupMessage#ack() acknowledge} completion of handling the message to send a response
 * back to the sender.
 * <pre>
 *   {@code
 *   // Join the group and run the given callback once successful
 *   group.join().thenAccept(member -> {
 *
 *     // Register a listener for the "hello" topic
 *     member.connection().onMessage("hello", message -> {
 *       // Handle the message and reply
 *       handleMessage(message);
 *       message.reply("Hello world!");
 *     });
 *
 *   });
 *   }
 * </pre>
 * It's critical that message listeners reply to messages, otherwise futures will be held in memory on the
 * sending side of the {@link GroupConnection connection} until the sender or receiver is removed from the
 * group.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupConnection {
  private final String memberId;
  private final Address address;
  private final GroupConnectionManager connections;

  GroupConnection(String memberId, Address address, GroupConnectionManager connections) {
    this.memberId = Assert.notNull(memberId, "memberId");
    this.address = address;
    this.connections = Assert.notNull(connections, "connections");
  }

  /**
   * Sends a direct message to the member.
   * <p>
   * Messages are ordered and replayed according to the semantics of the underlying {@link io.atomix.catalyst.transport.Transport}.
   * The connection does not inherently guarantee any order or exactly-once semantics.
   * <p>
   * The message will be sent to the member on the given {@code topic}. If no listener has been registered
   * by the member for the topic, the message will fail. If a listener exists for the topic, the message
   * will be handled by the listener and the {@link GroupMessage#reply(Object) reply} will be sent back
   * to this sender. Once the reply is received, the returned {@link CompletableFuture} will be completed
   * with the response from the member.
   *
   * @param topic The message topic.
   * @param message The message to send.
   * @param <T> The message type.
   * @param <U> The expected response type.
   * @return A completable future to be completed with the message response.
   * @throws NullPointerException if the {@code topic} is {@code null}
   */
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    if (address == null)
      return Futures.exceptionalFuture(new IllegalStateException("no address for member: " + memberId));
    return connections.getConnection(address).thenCompose(c -> c.send(new GroupMessage<>(memberId, topic, message)));
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s, address=%s]", getClass().getSimpleName(), memberId, address);
  }

}
