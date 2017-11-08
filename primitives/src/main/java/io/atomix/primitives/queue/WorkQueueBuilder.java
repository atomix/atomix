/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitives.queue;

import io.atomix.primitives.DistributedPrimitive.Type;
import io.atomix.primitives.DistributedPrimitiveBuilder;

/**
 * Work queue builder.
 */
public abstract class WorkQueueBuilder<E> extends DistributedPrimitiveBuilder<WorkQueueBuilder<E>, WorkQueue<E>, AsyncWorkQueue<E>> {

  public WorkQueueBuilder() {
    super(Type.WORK_QUEUE);
  }

  @Override
  public WorkQueue<E> build() {
    return buildAsync().asWorkQueue();
  }
}
