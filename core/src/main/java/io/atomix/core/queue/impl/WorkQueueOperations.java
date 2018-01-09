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
package io.atomix.core.queue.impl;

import com.google.common.base.MoreObjects;
import io.atomix.core.queue.Task;
import io.atomix.core.queue.WorkQueueStats;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

import java.util.Collection;

/**
 * {@link io.atomix.core.queue.WorkQueue} operations.
 * <p>
 * WARNING: Do not refactor enum values. Only add to them.
 * Changing values risk breaking the ability to backup/restore/upgrade clusters.
 */
public enum WorkQueueOperations implements OperationId {
  STATS(OperationType.QUERY),
  REGISTER(OperationType.COMMAND),
  UNREGISTER(OperationType.COMMAND),
  ADD(OperationType.COMMAND),
  TAKE(OperationType.COMMAND),
  COMPLETE(OperationType.COMMAND),
  CLEAR(OperationType.COMMAND);

  private final OperationType type;

  WorkQueueOperations(OperationType type) {
    this.type = type;
  }

  @Override
  public String id() {
    return name();
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(Add.class)
      .register(Take.class)
      .register(Complete.class)
      .register(Task.class)
      .register(WorkQueueStats.class)
      .build(WorkQueueOperations.class.getSimpleName());

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