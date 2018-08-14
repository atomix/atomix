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
package io.atomix.core.collection.impl;

import com.google.common.base.Throwables;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.iterator.SyncIterator;
import io.atomix.core.iterator.impl.BlockingIterator;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link DistributedCollection} that merely delegates to a {@link AsyncDistributedCollection} and
 * waits for the operation to complete.
 *
 * @param <E> collection element type
 */
public class BlockingDistributedCollection<E> extends Synchronous<AsyncDistributedCollection<E>> implements DistributedCollection<E> {

  private final long operationTimeoutMillis;

  private final AsyncDistributedCollection<E> asyncCollection;

  public BlockingDistributedCollection(AsyncDistributedCollection<E> asyncCollection, long operationTimeoutMillis) {
    super(asyncCollection);
    this.asyncCollection = asyncCollection;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int size() {
    return complete(asyncCollection.size());
  }

  @Override
  public boolean isEmpty() {
    return complete(asyncCollection.isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object o) {
    return complete(asyncCollection.contains((E) o));
  }

  @Override
  public boolean add(E e) {
    return complete(asyncCollection.add(e));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object o) {
    return complete(asyncCollection.remove((E) o));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean containsAll(Collection<?> c) {
    return complete(asyncCollection.containsAll((Collection<? extends E>) c));
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return complete(asyncCollection.addAll(c));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean retainAll(Collection<?> c) {
    return complete(asyncCollection.retainAll((Collection<? extends E>) c));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean removeAll(Collection<?> c) {
    return complete(asyncCollection.removeAll((Collection<? extends E>) c));
  }

  @Override
  public void clear() {
    complete(asyncCollection.clear());
  }

  @Override
  public SyncIterator<E> iterator() {
    return new BlockingIterator<>(asyncCollection.iterator(), operationTimeoutMillis);
  }

  @Override
  public void addListener(CollectionEventListener<E> listener) {
    complete(asyncCollection.addListener(listener));
  }

  @Override
  public void removeListener(CollectionEventListener<E> listener) {
    complete(asyncCollection.removeListener(listener));
  }

  @Override
  public Object[] toArray() {
    return stream().toArray();
  }

  @Override
  public <T> T[] toArray(T[] array) {
    Object[] copy = toArray();
    System.arraycopy(copy, 0, array, 0, Math.min(copy.length, array.length));
    return array;
  }

  @Override
  public AsyncDistributedCollection<E> async() {
    return asyncCollection;
  }

  protected <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else if (cause instanceof NoSuchElementException) {
        throw (NoSuchElementException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
