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
package io.atomix.core.set.impl;

import io.atomix.core.set.AsyncDistributedTreeSet;
import io.atomix.core.set.DistributedTreeSet;

/**
 * Implementation of {@link DistributedTreeSet} that merely delegates to a {@link AsyncDistributedTreeSet} and waits for
 * the operation to complete.
 *
 * @param <E> set element type
 */
public class BlockingDistributedTreeSet<E extends Comparable<E>> extends BlockingDistributedNavigableSet<E> implements DistributedTreeSet<E> {

  private final AsyncDistributedTreeSet<E> asyncSet;

  public BlockingDistributedTreeSet(AsyncDistributedTreeSet<E> asyncSet, long operationTimeoutMillis) {
    super(asyncSet, operationTimeoutMillis);
    this.asyncSet = asyncSet;
  }

  @Override
  public AsyncDistributedTreeSet<E> async() {
    return asyncSet;
  }
}
