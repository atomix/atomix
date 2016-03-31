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
package io.atomix.group.task.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.group.internal.GroupCommands;
import io.atomix.group.task.Task;
import io.atomix.group.util.Submitter;

import java.util.concurrent.CompletableFuture;

/**
 * Group member task.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTask<T> implements Task<T>, CatalystSerializable {
  private long id;
  private String member;
  private String type;
  private T value;
  private transient Submitter submitter;

  public GroupTask() {
  }

  public GroupTask(long id, String member, String type, T value) {
    this.id = id;
    this.member = member;
    this.type = type;
    this.value = value;
  }

  /**
   * Sets the task submitter.
   */
  GroupTask<T> setSubmitter(Submitter submitter) {
    this.submitter = submitter;
    return this;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the member to which the task is enqueued.
   *
   * @return The task member.
   */
  public String member() {
    return member;
  }

  /**
   * Returns the task type.
   *
   * @return The task type.
   */
  public String type() {
    return type;
  }

  @Override
  public T task() {
    return value;
  }

  @Override
  public CompletableFuture<Void> ack() {
    return submitter.submit(new GroupCommands.Ack(member, id, true));
  }

  @Override
  public CompletableFuture<Void> fail() {
    return submitter.submit(new GroupCommands.Ack(member, id, false));
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(id);
    buffer.writeString(member);
    buffer.writeString(type);
    serializer.writeObject(value, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    id = buffer.readLong();
    member = buffer.readString();
    type = buffer.readString();
    value = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[member=%s]", getClass().getSimpleName(), member);
  }

}
