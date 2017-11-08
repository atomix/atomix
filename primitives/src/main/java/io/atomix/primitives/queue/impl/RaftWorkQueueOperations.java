/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.queue.impl;

import com.google.common.base.MoreObjects;
import io.atomix.primitives.queue.Task;
import io.atomix.primitives.queue.WorkQueueStats;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import java.util.Collection;

/**
 * {@link RaftWorkQueue} resource state machine operations.
 */
public enum RaftWorkQueueOperations implements OperationId {
  STATS("stats", OperationType.QUERY),
  REGISTER("register", OperationType.COMMAND),
  UNREGISTER("unregister", OperationType.COMMAND),
  ADD("add", OperationType.COMMAND),
  TAKE("take", OperationType.COMMAND),
  COMPLETE("complete", OperationType.COMMAND),
  CLEAR("clear", OperationType.COMMAND);

  private final String id;
  private final OperationType type;

  RaftWorkQueueOperations(String id, OperationType type) {
    this.id = id;
    this.type = type;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(Add.class)
      .register(Take.class)
      .register(Complete.class)
      .register(Task.class)
      .register(WorkQueueStats.class)
      .build(RaftWorkQueueOperations.class.getSimpleName());

  /**
   * Work queue operation.
   */
  public abstract static class WorkQueueOperation {
  }

  /**
   * Command to add a collection of tasks to the queue.
   */
  @SuppressWarnings("serial")
  public static class Add extends WorkQueueOperation {
    private Collection<byte[]> items;

    private Add() {
    }

    public Add(Collection<byte[]> items) {
      this.items = items;
    }

    public Collection<byte[]> items() {
      return items;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("items", items)
          .toString();
    }
  }

  /**
   * Command to take a task from the queue.
   */
  @SuppressWarnings("serial")
  public static class Take extends WorkQueueOperation {
    private int maxTasks;

    private Take() {
    }

    public Take(int maxTasks) {
      this.maxTasks = maxTasks;
    }

    public int maxTasks() {
      return maxTasks;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("maxTasks", maxTasks)
          .toString();
    }
  }

  @SuppressWarnings("serial")
  public static class Complete extends WorkQueueOperation {
    private Collection<String> taskIds;

    private Complete() {
    }

    public Complete(Collection<String> taskIds) {
      this.taskIds = taskIds;
    }

    public Collection<String> taskIds() {
      return taskIds;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("taskIds", taskIds)
          .toString();
    }
  }
}