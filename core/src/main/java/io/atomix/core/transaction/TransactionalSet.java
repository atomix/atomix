/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.transaction;

import io.atomix.primitive.SyncPrimitive;

/**
 * Transactional set.
 */
public interface TransactionalSet<E> extends SyncPrimitive {

  /**
   * Adds the specified element to this set if it is not already present
   * (optional operation).  More formally, adds the specified element
   * <code>e</code> to this set if the set contains no element <code>e2</code>
   * such that
   * <code>(e==null&nbsp;?&nbsp;e2==null&nbsp;:&nbsp;e.equals(e2))</code>.
   * If this set already contains the element, the call leaves the set
   * unchanged and returns <code>false</code>.  In combination with the
   * restriction on constructors, this ensures that sets never contain
   * duplicate elements.
   *
   * @param e element to be added to this set
   * @return <code>true</code> if this set did not already contain the specified
   * element
   */
  boolean add(E e);

  /**
   * Removes the specified element from this set if it is present
   * (optional operation).  More formally, removes an element <code>e</code>
   * such that
   * <code>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</code>, if
   * this set contains such an element.  Returns <code>true</code> if this set
   * contained the element (or equivalently, if this set changed as a
   * result of the call).  (This set will not contain the element once the
   * call returns.)
   *
   * @param e element to be removed to this set
   * @return <code>true</code> if this set contained the specified element
   */
  boolean remove(E e);

  /**
   * Returns <code>true</code> if this set contains the specified element.
   * More formally, returns <code>true</code> if and only if this set
   * contains an element <code>e</code> such that
   * <code>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</code>.
   *
   * @param e element whose presence in this set is to be tested
   * @return <code>true</code> if this set contains the specified element
   * @throws ClassCastException   if the type of the specified element
   *                              is incompatible with this set
   * @throws NullPointerException if the specified element is null and this
   *                              set does not permit null elements
   */
  boolean contains(E e);

  @Override
  AsyncTransactionalSet<E> async();
}
