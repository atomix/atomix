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
package io.atomix.core.set.impl;

import io.atomix.core.set.AsyncDistributedTreeSet;
import io.atomix.core.set.DistributedTreeSet;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.NavigableSet;

/**
 * Asynchronous distributed Java-backed tree set.
 */
public class AsyncDistributedJavaTreeSet<E extends Comparable<E>> extends AsyncDistributedNavigableJavaSet<E> implements AsyncDistributedTreeSet<E> {
  public AsyncDistributedJavaTreeSet(String name, PrimitiveProtocol protocol, NavigableSet<E> set) {
    super(name, protocol, set);
  }

  @Override
  public DistributedTreeSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedTreeSet<>(this, operationTimeout.toMillis());
  }
}
