// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
