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
package io.atomix.primitives.set.impl;

import io.atomix.primitives.PrimitiveException;
import io.atomix.primitives.Synchronous;
import io.atomix.primitives.set.AsyncDistributedSet;
import io.atomix.primitives.set.DistributedSet;
import io.atomix.primitives.set.SetEventListener;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link DistributedSet} that merely delegates to a {@link AsyncDistributedSet}
 * and waits for the operation to complete.
 *
 * @param <E> set element type
 */
public class DefaultDistributedSet<E> extends Synchronous<AsyncDistributedSet<E>> implements DistributedSet<E> {

  private final long operationTimeoutMillis;

  private final AsyncDistributedSet<E> asyncSet;

  public DefaultDistributedSet(AsyncDistributedSet<E> asyncSet, long operationTimeoutMillis) {
    super(asyncSet);
    this.asyncSet = asyncSet;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int size() {
    return complete(asyncSet.size());
  }

  @Override
  public boolean isEmpty() {
    return complete(asyncSet.isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object o) {
    return complete(asyncSet.contains((E) o));
  }

  @Override
  public Iterator<E> iterator() {
    return complete(asyncSet.getAsImmutableSet()).iterator();
  }

  @Override
  public Object[] toArray() {
    return complete(asyncSet.getAsImmutableSet()).stream().toArray();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] a) {
    // TODO: Optimize this to only allocate a new array if the set size
    // is larger than the array.length. If the set size is smaller than
    // the array.length then copy the data into the array and set the
    // last element in the array to be null.
    final T[] resizedArray =
        (T[]) Array.newInstance(a.getClass().getComponentType(), complete(asyncSet.getAsImmutableSet()).size());
    return complete(asyncSet.getAsImmutableSet()).toArray(resizedArray);
  }

  @Override
  public boolean add(E e) {
    return complete(asyncSet.add(e));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object o) {
    return complete(asyncSet.remove((E) o));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean containsAll(Collection<?> c) {
    return complete(asyncSet.containsAll((Collection<? extends E>) c));
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return complete(asyncSet.addAll(c));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean retainAll(Collection<?> c) {
    return complete(asyncSet.retainAll((Collection<? extends E>) c));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean removeAll(Collection<?> c) {
    return complete(asyncSet.removeAll((Collection<? extends E>) c));
  }

  @Override
  public void clear() {
    complete(asyncSet.clear());
  }

  @Override
  public void addListener(SetEventListener<E> listener) {
    complete(asyncSet.addListener(listener));
  }

  @Override
  public void removeListener(SetEventListener<E> listener) {
    complete(asyncSet.removeListener(listener));
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof PrimitiveException) {
        throw (PrimitiveException) e.getCause();
      } else {
        throw new PrimitiveException(e.getCause());
      }
    }
  }
}
