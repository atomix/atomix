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
package io.atomix.core.list.impl;

import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * Blocking distributed list.
 */
public class BlockingDistributedList<E> extends BlockingDistributedCollection<E> implements DistributedList<E> {
  private final AsyncDistributedList<E> asyncList;

  public BlockingDistributedList(AsyncDistributedList<E> asyncList, long operationTimeoutMillis) {
    super(asyncList, operationTimeoutMillis);
    this.asyncList = asyncList;
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    return complete(asyncList.addAll(index, c));
  }

  @Override
  public E get(int index) {
    return complete(asyncList.get(index));
  }

  @Override
  public E set(int index, E element) {
    return complete(asyncList.set(index, element));
  }

  @Override
  public void add(int index, E element) {
    complete(asyncList.add(index, element));
  }

  @Override
  public E remove(int index) {
    return complete(asyncList.remove(index));
  }

  @Override
  public int indexOf(Object o) {
    return complete(asyncList.indexOf(o));
  }

  @Override
  public int lastIndexOf(Object o) {
    return complete(asyncList.lastIndexOf(o));
  }

  @Override
  public ListIterator<E> listIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncDistributedList<E> async() {
    return asyncList;
  }
}
