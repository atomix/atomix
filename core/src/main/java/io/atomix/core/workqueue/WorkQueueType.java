// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.workqueue;

import io.atomix.core.workqueue.impl.DefaultWorkQueueBuilder;
import io.atomix.core.workqueue.impl.DefaultWorkQueueService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Work queue primitive type.
 */
public class WorkQueueType<E> implements PrimitiveType<WorkQueueBuilder<E>, WorkQueueConfig, WorkQueue<E>> {
  private static final String NAME = "work-queue";
  private static final WorkQueueType INSTANCE = new WorkQueueType();

  /**
   * Returns a new work queue type instance.
   *
   * @param <E> the element type
   * @return a new work queue type
   */
  @SuppressWarnings("unchecked")
  public static <E> WorkQueueType<E> instance() {
    return INSTANCE;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Namespace namespace() {
    return Namespace.builder()
        .register(PrimitiveType.super.namespace())
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
        .register(Task.class)
        .register(WorkQueueStats.class)
        .build();
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    return new DefaultWorkQueueService();
  }

  @Override
  public WorkQueueConfig newConfig() {
    return new WorkQueueConfig();
  }

  @Override
  public WorkQueueBuilder<E> newBuilder(String name, WorkQueueConfig config, PrimitiveManagementService managementService) {
    return new DefaultWorkQueueBuilder<>(name, config, managementService);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .toString();
  }
}
