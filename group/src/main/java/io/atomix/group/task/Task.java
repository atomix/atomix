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
package io.atomix.group.task;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.group.DistributedGroup;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a reliable task received by a member to be processed and acknowledged.
 * <p>
 * Tasks are {@link TaskQueue#submit(Object) submitted} by {@link DistributedGroup} users to any member of a group.
 * Tasks are replicated and persisted within the Atomix cluster before being pushed to clients on a queue. Once a task
 * is received by a task listener, the task may be processed asynchronously and either {@link #ack() acknowledged} or
 * {@link #fail() failed} once processing is complete.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("task-group").get();
 *   group.join().thenAccept(member -> {
 *     member.tasks().onTask(task -> {
 *       processTask(task).thenRun(() -> {
 *         task.ack();
 *       });
 *     });
 *   });
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Task<T> implements CatalystSerializable {
  private long id;
  private String member;
  private T value;
  private transient CompletableFuture<Boolean> future;

  public Task() {
  }

  public Task(long id, String member, T value) {
    this.id = id;
    this.member = member;
    this.value = value;
  }

  Task<T> setFuture(CompletableFuture<Boolean> future) {
    this.future = future;
    return this;
  }

  /**
   * Returns the task ID.
   * <p>
   * The task ID is guaranteed to be unique and monotonically increasing within a given task queue. Tasks received
   * across members are not associated with one another.
   *
   * @return The monotonically increasing task ID.
   */
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
   * Returns the task value.
   * <p>
   * This is the value that was {@link TaskQueue#submit(Object) submitted} by the sending process.
   *
   * @return The task value.
   */
  public T task() {
    return value;
  }

  /**
   * Acknowledges completion of the task.
   * <p>
   * Once a task is acknowledged, an ack will be sent back to the process that submitted the task. Acknowledging
   * completion of a task does not guarantee that the sender will learn of the acknowledgement. The acknowledgement
   * itself may fail to reach the cluster or the sender may crash before the acknowledgement can be received.
   * Acks serve only as positive acknowledgement, but the lack of an ack does not indicate failure.
   */
  public void ack() {
    future.complete(true);
  }

  /**
   * Fails processing of the task.
   * <p>
   * Once a task is failed, a failure message will be sent back to the process that submitted the task for processing.
   * Failing a task does not guarantee that the sender will learn of the failure. The process that submitted the task
   * may itself fail.
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
