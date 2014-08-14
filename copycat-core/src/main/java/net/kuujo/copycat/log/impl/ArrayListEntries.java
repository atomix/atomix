/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import net.kuujo.copycat.log.Entries;
import net.kuujo.copycat.log.Entry;

/**
 * Array list entries implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ArrayListEntries<U extends Entry> implements Entries<U> {
  private final List<U> entries;

  public ArrayListEntries() {
    this.entries = new ArrayList<>();
  }

  public ArrayListEntries(List<U> entries) {
    this.entries = new ArrayList<>(entries);
  }

  public ArrayListEntries(@SuppressWarnings("unchecked") U... entries) {
    this.entries = Arrays.asList(entries);
  }

  @Override
  public int size() {
    return entries.size();
  }

  @Override
  public boolean isEmpty() {
    return entries.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return entries.contains(o);
  }

  @Override
  public Iterator<U> iterator() {
    return entries.iterator();
  }

  @Override
  public Object[] toArray() {
    return entries.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return entries.toArray(a);
  }

  @Override
  public boolean add(U e) {
    return entries.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return entries.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return entries.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends U> c) {
    return entries.addAll(c);
  }

  @Override
  public boolean addAll(int index, Collection<? extends U> c) {
    return entries.addAll(index, c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return entries.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return entries.retainAll(c);
  }

  @Override
  public void clear() {
    entries.clear();
  }

  @Override
  public U get(int index) {
    return entries.get(index);
  }

  @Override
  public <T extends U> T get(int index, Class<T> type) {
    return type.cast(entries.get(index));
  }

  @Override
  public U set(int index, U element) {
    return entries.set(index, element);
  }

  @Override
  public void add(int index, U element) {
    entries.add(index, element);
  }

  @Override
  public U remove(int index) {
    return entries.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    return entries.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return entries.lastIndexOf(o);
  }

  @Override
  public ListIterator<U> listIterator() {
    return entries.listIterator();
  }

  @Override
  public ListIterator<U> listIterator(int index) {
    return entries.listIterator(index);
  }

  @Override
  public List<U> subList(int fromIndex, int toIndex) {
    return new ArrayListEntries<U>(entries.subList(fromIndex, toIndex));
  }

  @Override
  public <T extends U> List<T> subList(int fromIndex, int toIndex, Class<T> type) {
    List<T> entries = new ArrayList<>();
    for (int i = fromIndex; i < toIndex; i++) {
      entries.add(type.cast(this.entries.get(i)));
    }
    return new ArrayListEntries<T>(entries);
  }

}
