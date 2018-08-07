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
package io.atomix.core.collection.impl;

import io.atomix.core.iterator.impl.IterableService;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

import java.util.Collection;

/**
 * Distributed collection service.
 */
public interface DistributedCollectionService<E> extends IterableService<E> {

  /**
   * Returns the number of elements in this collection.  If this collection
   * contains more than <code>Integer.MAX_VALUE</code> elements, returns
   * <code>Integer.MAX_VALUE</code>.
   *
   * @return the number of elements in this collection
   */
  @Query
  int size();

  /**
   * Returns <code>true</code> if this collection contains no elements.
   *
   * @return <code>true</code> if this collection contains no elements
   */
  @Query
  boolean isEmpty();

  /**
   * Returns <code>true</code> if this collection contains the specified element.
   * More formally, returns <code>true</code> if and only if this collection
   * contains at least one element <code>e</code> such that
   * <code>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</code>.
   *
   * @param o element whose presence in this collection is to be tested
   * @return <code>true</code> if this collection contains the specified
   *         element
   * @throws ClassCastException if the type of the specified element
   *         is incompatible with this collection
   *         (<a href="#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified element is null and this
   *         collection does not permit null elements
   *         (<a href="#optional-restrictions">optional</a>)
   */
  @Query
  boolean contains(E o);

  /**
   * Ensures that this collection contains the specified element (optional
   * operation).  Returns <code>true</code> if this collection changed as a
   * result of the call.  (Returns <code>false</code> if this collection does
   * not permit duplicates and already contains the specified element.)<p>
   *
   * Collections that support this operation may place limitations on what
   * elements may be added to this collection.  In particular, some
   * collections will refuse to add <code>null</code> elements, and others will
   * impose restrictions on the type of elements that may be added.
   * Collection classes should clearly specify in their documentation any
   * restrictions on what elements may be added.<p>
   *
   * If a collection refuses to add a particular element for any reason
   * other than that it already contains the element, it <i>must</i> throw
   * an exception (rather than returning <code>false</code>).  This preserves
   * the invariant that a collection always contains the specified element
   * after this call returns.
   *
   * @param element element whose presence in this collection is to be ensured
   * @return <code>true</code> if this collection changed as a result of the
   *         call
   * @throws UnsupportedOperationException if the <code>add</code> operation
   *         is not supported by this collection
   * @throws ClassCastException if the class of the specified element
   *         prevents it from being added to this collection
   * @throws NullPointerException if the specified element is null and this
   *         collection does not permit null elements
   * @throws IllegalArgumentException if some property of the element
   *         prevents it from being added to this collection
   * @throws IllegalStateException if the element cannot be added at this
   *         time due to insertion restrictions
   */
  @Command
  CollectionUpdateResult<Boolean> add(E element);

  /**
   * Removes a single instance of the specified element from this
   * collection, if it is present (optional operation).  More formally,
   * removes an element <code>e</code> such that
   * <code>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</code>, if
   * this collection contains one or more such elements.  Returns
   * <code>true</code> if this collection contained the specified element (or
   * equivalently, if this collection changed as a result of the call).
   *
   * @param o element to be removed from this collection, if present
   * @return <code>true</code> if an element was removed as a result of this call
   * @throws ClassCastException if the type of the specified element
   *         is incompatible with this collection
   *         (<a href="#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified element is null and this
   *         collection does not permit null elements
   *         (<a href="#optional-restrictions">optional</a>)
   * @throws UnsupportedOperationException if the <code>remove</code> operation
   *         is not supported by this collection
   */
  @Command("removeValue")
  CollectionUpdateResult<Boolean> remove(E o);

  /**
   * Returns <code>true</code> if this collection contains all of the elements
   * in the specified collection.
   *
   * @param  c collection to be checked for containment in this collection
   * @return <code>true</code> if this collection contains all of the elements
   *         in the specified collection
   * @throws ClassCastException if the types of one or more elements
   *         in the specified collection are incompatible with this
   *         collection
   *         (<a href="#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified collection contains one
   *         or more null elements and this collection does not permit null
   *         elements
   *         (<a href="#optional-restrictions">optional</a>),
   *         or if the specified collection is null.
   * @see    #contains(Object)
   */
  @Query
  boolean containsAll(Collection<?> c);

  /**
   * Adds all of the elements in the specified collection to this collection
   * (optional operation).  The behavior of this operation is undefined if
   * the specified collection is modified while the operation is in progress.
   * (This implies that the behavior of this call is undefined if the
   * specified collection is this collection, and this collection is
   * nonempty.)
   *
   * @param c collection containing elements to be added to this collection
   * @return <code>true</code> if this collection changed as a result of the call
   * @throws UnsupportedOperationException if the <code>addAll</code> operation
   *         is not supported by this collection
   * @throws ClassCastException if the class of an element of the specified
   *         collection prevents it from being added to this collection
   * @throws NullPointerException if the specified collection contains a
   *         null element and this collection does not permit null elements,
   *         or if the specified collection is null
   * @throws IllegalArgumentException if some property of an element of the
   *         specified collection prevents it from being added to this
   *         collection
   * @throws IllegalStateException if not all the elements can be added at
   *         this time due to insertion restrictions
   * @see #add(Object)
   */
  @Command
  CollectionUpdateResult<Boolean> addAll(Collection<? extends E> c);

  /**
   * Retains only the elements in this collection that are contained in the
   * specified collection (optional operation).  In other words, removes from
   * this collection all of its elements that are not contained in the
   * specified collection.
   *
   * @param c collection containing elements to be retained in this collection
   * @return <code>true</code> if this collection changed as a result of the call
   * @throws UnsupportedOperationException if the <code>retainAll</code> operation
   *         is not supported by this collection
   * @throws ClassCastException if the types of one or more elements
   *         in this collection are incompatible with the specified
   *         collection
   *         (<a href="#optional-restrictions">optional</a>)
   * @throws NullPointerException if this collection contains one or more
   *         null elements and the specified collection does not permit null
   *         elements
   *         (<a href="#optional-restrictions">optional</a>),
   *         or if the specified collection is null
   * @see #remove(Object)
   * @see #contains(Object)
   */
  @Command
  CollectionUpdateResult<Boolean> retainAll(Collection<?> c);

  /**
   * Removes all of this collection's elements that are also contained in the
   * specified collection (optional operation).  After this call returns,
   * this collection will contain no elements in common with the specified
   * collection.
   *
   * @param c collection containing elements to be removed from this collection
   * @return <code>true</code> if this collection changed as a result of the
   *         call
   * @throws UnsupportedOperationException if the <code>removeAll</code> method
   *         is not supported by this collection
   * @throws ClassCastException if the types of one or more elements
   *         in this collection are incompatible with the specified
   *         collection
   *         (<a href="#optional-restrictions">optional</a>)
   * @throws NullPointerException if this collection contains one or more
   *         null elements and the specified collection does not support
   *         null elements
   *         (<a href="#optional-restrictions">optional</a>),
   *         or if the specified collection is null
   * @see #remove(Object)
   * @see #contains(Object)
   */
  @Command
  CollectionUpdateResult<Boolean> removeAll(Collection<?> c);

  /**
   * Removes all of the elements from this collection (optional operation).
   * The collection will be empty after this method returns.
   *
   * @throws UnsupportedOperationException if the <code>clear</code> operation
   *         is not supported by this collection
   */
  @Command
  CollectionUpdateResult<Void> clear();

  /**
   * Adds a listener to the service.
   */
  @Command
  void listen();

  /**
   * Stops listening for events from the service.
   */
  @Command
  void unlisten();

}
