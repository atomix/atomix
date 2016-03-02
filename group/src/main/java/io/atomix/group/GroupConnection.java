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
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.Futures;

import java.util.concurrent.CompletableFuture;

/**
 * Facilitates direct communication between group members.
 * <p>
 * Members in a {@link DistributedGroup} can communicate with each other directly via a connection.
 * To send a direct message to a remote {@link GroupMember}, the member must have been configured with
 * an {@link Address} to which to bind its server.
 * <pre>
 *   {@code
 *   group.member("foo").send("user", "1").thenAccept(reply -> {
 *     ...
 *   });
 *   }
 * </pre>
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
   * The returned {@link CompletableFuture} will be completed with the response from the member.
   *
   * @param topic The message topic.
   * @param message The message to send.
   * @param <T> The message type.
   * @param <U> The expected response type.
   * @return A completable future to be completed with the message response.
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
