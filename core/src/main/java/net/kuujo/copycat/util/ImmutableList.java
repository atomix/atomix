/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.util;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Immutable list.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ImmutableList<E> implements List<E> {
  private final List<E> list;

  public ImmutableList(List<E> list) {
    this.list = list;
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return list.contains(o);
  }

  @NotNull
  @Override
  public Iterator<E> iterator() {
    return list.iterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return list.toArray();
  }

  @NotNull
  @Override
  public <T> T[] toArray(T[] a) {
    return list.toArray(a);
  }

  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException("add not supported");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("remove not supported");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return list.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException("addAll not supported");
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    throw new UnsupportedOperationException("addAll not supported");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("removeAll not supported");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("retainAll not supported");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("clear not supported");
  }

  @Override
  public E get(int index) {
    return list.get(index);
  }

  @Override
  public E set(int index, E element) {
    throw new UnsupportedOperationException("set not supported");
  }

  @Override
  public void add(int index, E element) {
    throw new UnsupportedOperationException("add not supported");
  }

  @Override
  public E remove(int index) {
    throw new UnsupportedOperationException("remove not supported");
  }

  @Override
  public int indexOf(Object o) {
    return list.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return list.lastIndexOf(o);
  }

  @NotNull
  @Override
  public ListIterator<E> listIterator() {
    return list.listIterator();
  }

  @NotNull
  @Override
  public ListIterator<E> listIterator(int index) {
    return list.listIterator(index);
  }

  @NotNull
  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    return new ImmutableList<>(list.subList(fromIndex, toIndex));
  }

}
