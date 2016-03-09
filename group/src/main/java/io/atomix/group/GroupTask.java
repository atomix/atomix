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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Group task.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTask<T> implements CatalystSerializable {
  private long id;
  private String member;
  private T value;
  private transient CompletableFuture<Boolean> future;

  public GroupTask() {
  }

  public GroupTask(long id, String member, T value) {
    this.id = id;
    this.member = member;
    this.value = value;
  }

  GroupTask<T> setFuture(CompletableFuture<Boolean> future) {
    this.future = future;
    return this;
  }

  /**
   * Returns the monotonically increasing task ID.
   *
   * @return The unique task ID.
   */
  public long id() {
    return id;
  }

  /**
   * Returns the member to which the task is enqueued.
   *
   * @return The task member.
   */
  String member() {
    return member;
  }

  /**
   * Returns the value of the task.
   *
   * @return The value of the task.
   */
  public T value() {
    return value;
  }

  /**
   * Acknowledges completion of the task.
   */
  public void ack() {
    future.complete(true);
  }

  /**
   * Fails processing of the task.
   */
  public void fail() {
    future.complete(false);
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(id);
    buffer.writeString(member);
    serializer.writeObject(value, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    id = buffer.readLong();
    member = buffer.readString();
    value = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), member);
  }

}
