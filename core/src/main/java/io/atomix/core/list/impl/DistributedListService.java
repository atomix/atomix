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

import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.collection.impl.DistributedCollectionService;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

import java.util.Collection;

/**
 * Distributed list service.
 */
public interface DistributedListService extends DistributedCollectionService<String> {

  /**
   * Inserts all of the elements in the specified collection into this
   * list at the specified position (optional operation).  Shifts the
   * element currently at that position (if any) and any subsequent
   * elements to the right (increases their indices).  The new elements
   * will appear in this list in the order that they are returned by the
   * specified collection's iterator.  The behavior of this operation is
   * undefined if the specified collection is modified while the
   * operation is in progress.  (Note that this will occur if the specified
   * collection is this list, and it's nonempty.)
   *
   * @param index index at which to insert the first element from the
   *              specified collection
   * @param c collection containing elements to be added to this list
   * @return <code>true</code> if this list changed as a result of the call
   * @throws UnsupportedOperationException if the <code>addAll</code> operation
   *         is not supported by this list
   * @throws ClassCastException if the class of an element of the specified
   *         collection prevents it from being added to this list
   * @throws NullPointerException if the specified collection contains one
   *         or more null elements and this list does not permit null
   *         elements, or if the specified collection is null
   * @throws IllegalArgumentException if some property of an element of the
   *         specified collection prevents it from being added to this list
   * @throws IndexOutOfBoundsException if the index is out of range
   *         (<code>index &lt; 0 || index &gt; size()</code>)
   */
  @Command("addAllIndex")
  CollectionUpdateResult<Boolean> addAll(int index, Collection<? extends String> c);

  /**
   * Returns the element at the specified position in this list.
   *
   * @param index index of the element to return
   * @return the element at the specified position in this list
   * @throws IndexOutOfBoundsException if the index is out of range
   *         (<code>index &lt; 0 || index &gt;= size()</code>)
   */
  @Query("getIndex")
  String get(int index);

  /**
   * Replaces the element at the specified position in this list with the
   * specified element (optional operation).
   *
   * @param index index of the element to replace
   * @param element element to be stored at the specified position
   * @return the element previously at the specified position
   * @throws UnsupportedOperationException if the <code>set</code> operation
   *         is not supported by this list
   * @throws ClassCastException if the class of the specified element
   *         prevents it from being added to this list
   * @throws NullPointerException if the specified element is null and
   *         this list does not permit null elements
   * @throws IllegalArgumentException if some property of the specified
   *         element prevents it from being added to this list
   * @throws IndexOutOfBoundsException if the index is out of range
   *         (<code>index &lt; 0 || index &gt;= size()</code>)
   */
  @Command("setIndex")
  CollectionUpdateResult<String> set(int index, String element);

  /**
   * Inserts the specified element at the specified position in this list
   * (optional operation).  Shifts the element currently at that position
   * (if any) and any subsequent elements to the right (adds one to their
   * indices).
   *
   * @param index index at which the specified element is to be inserted
   * @param element element to be inserted
   * @throws UnsupportedOperationException if the <code>add</code> operation
   *         is not supported by this list
   * @throws ClassCastException if the class of the specified element
   *         prevents it from being added to this list
   * @throws NullPointerException if the specified element is null and
   *         this list does not permit null elements
   * @throws IllegalArgumentException if some property of the specified
   *         element prevents it from being added to this list
   * @throws IndexOutOfBoundsException if the index is out of range
   *         (<code>index &lt; 0 || index &gt; size()</code>)
   */
  @Command("addIndex")
  CollectionUpdateResult<Void> add(int index, String element);

  /**
   * Removes the element at the specified position in this list (optional
   * operation).  Shifts any subsequent elements to the left (subtracts one
   * from their indices).  Returns the element that was removed from the
   * list.
   *
   * @param index the index of the element to be removed
   * @return the element previously at the specified position
   * @throws UnsupportedOperationException if the <code>remove</code> operation
   *         is not supported by this list
   * @throws IndexOutOfBoundsException if the index is out of range
   *         (<code>index &lt; 0 || index &gt;= size()</code>)
   */
  @Command("removeIndex")
  CollectionUpdateResult<String> remove(int index);

  /**
   * Returns the index of the first occurrence of the specified element
   * in this list, or -1 if this list does not contain the element.
   * More formally, returns the lowest index <code>i</code> such that
   * <code>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</code>,
   * or -1 if there is no such index.
   *
   * @param o element to search for
   * @return the index of the first occurrence of the specified element in
   *         this list, or -1 if this list does not contain the element
   * @throws ClassCastException if the type of the specified element
   *         is incompatible with this list
   *         (<a href="Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified element is null and this
   *         list does not permit null elements
   *         (<a href="Collection.html#optional-restrictions">optional</a>)
   */
  @Query
  int indexOf(Object o);

  /**
   * Returns the index of the last occurrence of the specified element
   * in this list, or -1 if this list does not contain the element.
   * More formally, returns the highest index <code>i</code> such that
   * <code>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</code>,
   * or -1 if there is no such index.
   *
   * @param o element to search for
   * @return the index of the last occurrence of the specified element in
   *         this list, or -1 if this list does not contain the element
   * @throws ClassCastException if the type of the specified element
   *         is incompatible with this list
   *         (<a href="Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified element is null and this
   *         list does not permit null elements
   *         (<a href="Collection.html#optional-restrictions">optional</a>)
   */
  @Query
  int lastIndexOf(Object o);

}
