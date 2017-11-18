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
package io.atomix.primitives.queue.impl;

import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitives.queue.AsyncWorkQueue;

/**
 * Discrete work queue builder.
 */
public class DiscreteWorkQueueBuilder<E> extends AbstractWorkQueueBuilder<E> {
  private final PrimitiveClient client;

  public DiscreteWorkQueueBuilder(String name, PrimitiveClient client) {
    super(name);
    this.client = client;
  }

  @Override
  public AsyncWorkQueue<E> buildAsync() {
    return newWorkQueue(client);
  }
}
