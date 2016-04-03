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
package io.atomix.group.messaging.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.group.internal.GroupCommands;
import io.atomix.group.messaging.Message;

import java.util.concurrent.CompletableFuture;

/**
 * Group member message.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupMessage<T> implements Message<T>, CatalystSerializable {
  private long id;
  private String member;
  private String queue;
  private T value;
  private transient MessageConsumerService consumerService;

  public GroupMessage() {
  }

  public GroupMessage(long id, String member, String queue, T value) {
    this.id = id;
    this.member = member;
    this.queue = queue;
    this.value = value;
  }

  /**
   * Sets the message consumer service.
   *
   * @param consumerService The message consumer service.
   * @return The group message.
   */
  GroupMessage<T> setConsumerService(MessageConsumerService consumerService) {
    this.consumerService = consumerService;
    return this;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the member to which the message is enqueued.
   *
   * @return The message member.
   */
  public String member() {
    return member;
  }

  /**
   * Returns the message queue.
   *
   * @return The message queue.
   */
  public String queue() {
    return queue;
  }

  @Override
  public T message() {
    return value;
  }

  @Override
  public CompletableFuture<Void> reply(Object message) {
    return consumerService.reply(new GroupCommands.Reply(member, queue, id, true, message));
  }

  @Override
  public CompletableFuture<Void> ack() {
    return consumerService.reply(new GroupCommands.Reply(member, queue, id, true, null));
  }

  @Override
  public CompletableFuture<Void> fail() {
    return consumerService.reply(new GroupCommands.Reply(member, queue, id, false, null));
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(id);
    buffer.writeString(member);
    buffer.writeString(queue);
    serializer.writeObject(value, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    id = buffer.readLong();
    member = buffer.readString();
    queue = buffer.readString();
    value = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), member);
  }

}
