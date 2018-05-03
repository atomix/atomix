/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import com.google.common.base.MoreObjects;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

public enum DistributedSemaphoreOperations implements OperationId {
  ACQUIRE(OperationType.COMMAND),
  RELEASE(OperationType.COMMAND),
  DRAIN(OperationType.COMMAND),
  INCREASE(OperationType.COMMAND),
  REDUCE(OperationType.COMMAND),
  AVAILABLE(OperationType.QUERY),
  QUEUE_STATUS(OperationType.QUERY),
  HOLDER_STATUS(OperationType.QUERY);

  private final OperationType type;

  DistributedSemaphoreOperations(OperationType type) {
    this.type = type;
  }

  @Override
  public OperationType type() {
    return type;
  }

  @Override
  public String id() {
    return name();
  }


  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
          .register(SemaphoreOperation.class)
          .register(Init.class)
          .register(Acquire.class)
          .register(Release.class)
          .register(Drain.class)
          .register(Increase.class)
          .register(Reduce.class)
          .build(DistributedSemaphoreOperations.class.getSimpleName());


  private static abstract class SemaphoreOperation {
    private long id;

    SemaphoreOperation(long id) {
      this.id = id;
    }

    public long id() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .toString();
    }
  }

  public static class Init extends SemaphoreOperation {
    private int initCapacity;

    Init(long id, int initCapacity) {
      super(id);
      this.initCapacity = initCapacity;
    }

    int initCapacity() {
      return initCapacity;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id())
              .add("initCapacity", initCapacity)
              .toString();
    }
  }

  public static class Acquire extends SemaphoreOperation {
    private int permits;
    private long timeout;

    Acquire(long id, int permits, long timeout) {
      super(id);
      this.permits = permits;
      this.timeout = timeout;
    }

    int permits() {
      return permits;
    }

    public long timeout() {
      return timeout;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id())
              .add("permits", permits)
              .add("timeout", timeout)
              .toString();
    }
  }

  public static class Release extends SemaphoreOperation {
    private int permits;

    Release(long id, int permits) {
      super(id);
      this.permits = permits;
    }

    int permits() {
      return permits;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id())
              .add("permits", permits)
              .toString();
    }
  }

  public static class Drain extends SemaphoreOperation {
    Drain(long id) {
      super(id);
    }
  }

  public static class Increase extends SemaphoreOperation {
    private int permits;

    public Increase(long id, int permits) {
      super(id);
      this.permits = permits;
    }

    int permits() {
      return permits;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id())
              .add("permits", permits)
              .toString();
    }
  }

  public static class Reduce extends SemaphoreOperation {
    private int permits;

    public Reduce(long id, int permits) {
      super(id);
      this.permits = permits;
    }

    int permits() {
      return permits;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", id())
              .add("permits", permits)
              .toString();
    }
  }

}
