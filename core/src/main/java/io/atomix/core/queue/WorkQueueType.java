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
package io.atomix.core.queue;

import io.atomix.core.queue.impl.WorkQueueProxyBuilder;
import io.atomix.core.queue.impl.WorkQueueService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Work queue primitive type.
 */
public class WorkQueueType<E> implements PrimitiveType<WorkQueueBuilder<E>, WorkQueue<E>> {
  private static final String NAME = "WORK_QUEUE";

  /**
   * Returns a new work queue type instance.
   *
   * @param <E> the element type
   * @return a new work queue type
   */
  public static <E> WorkQueueType<E> instance() {
    return new WorkQueueType<>();
  }

  private WorkQueueType() {
  }

  @Override
  public String id() {
    return NAME;
  }

  @Override
  public PrimitiveService newService() {
    return new WorkQueueService();
  }

  @Override
  public WorkQueueBuilder<E> newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    return new WorkQueueProxyBuilder<>(name, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .toString();
  }
}
