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

import io.atomix.core.set.DistributedSetType;
import io.atomix.primitive.SyncPrimitive;

/**
 * Transactional set.
 */
public interface TransactionalSet<E> extends SyncPrimitive {

  @Override
  default DistributedSetType<E> primitiveType() {
    return DistributedSetType.instance();
  }

  /**
   * Adds the specified element to this set if it is not already present
   * (optional operation).  More formally, adds the specified element
   * <tt>e</tt> to this set if the set contains no element <tt>e2</tt>
   * such that
   * <tt>(e==null&nbsp;?&nbsp;e2==null&nbsp;:&nbsp;e.equals(e2))</tt>.
   * If this set already contains the element, the call leaves the set
   * unchanged and returns <tt>false</tt>.  In combination with the
   * restriction on constructors, this ensures that sets never contain
   * duplicate elements.
   *
   * @param e element to be added to this set
   * @return <tt>true</tt> if this set did not already contain the specified
   * element
   */
  boolean add(E e);

  /**
   * Removes the specified element from this set if it is present
   * (optional operation).  More formally, removes an element <tt>e</tt>
   * such that
   * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>, if
   * this set contains such an element.  Returns <tt>true</tt> if this set
   * contained the element (or equivalently, if this set changed as a
   * result of the call).  (This set will not contain the element once the
   * call returns.)
   *
   * @param e element to be removed to this set
   * @return <tt>true</tt> if this set contained the specified element
   */
  boolean remove(E e);

  /**
   * Returns <tt>true</tt> if this set contains the specified element.
   * More formally, returns <tt>true</tt> if and only if this set
   * contains an element <tt>e</tt> such that
   * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
   *
   * @param e element whose presence in this set is to be tested
   * @return <tt>true</tt> if this set contains the specified element
   * @throws ClassCastException   if the type of the specified element
   *                              is incompatible with this set
   * @throws NullPointerException if the specified element is null and this
   *                              set does not permit null elements
   */
  boolean contains(E e);

  @Override
  AsyncTransactionalSet<E> async();
}
