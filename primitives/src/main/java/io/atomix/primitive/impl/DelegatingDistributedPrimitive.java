/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.impl;

import com.google.common.base.MoreObjects;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveType;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for primitive delegates.
 */
public abstract class DelegatingDistributedPrimitive implements AsyncPrimitive {
  private final AsyncPrimitive primitive;

  public DelegatingDistributedPrimitive(AsyncPrimitive primitive) {
    this.primitive = checkNotNull(primitive);
  }

  @Override
  public String name() {
    return primitive.name();
  }

  @Override
  public PrimitiveType primitiveType() {
    return primitive.primitiveType();
  }

  @Override
  public CompletableFuture<Void> destroy() {
    return primitive.destroy();
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    primitive.addStatusChangeListener(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    primitive.removeStatusChangeListener(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return primitive.statusChangeListeners();
  }

  @Override
  public CompletableFuture<Void> close() {
    return primitive.close();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("delegate", primitive)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(primitive);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof DelegatingDistributedPrimitive
        && primitive.equals(((DelegatingDistributedPrimitive) other).primitive);
  }
}
