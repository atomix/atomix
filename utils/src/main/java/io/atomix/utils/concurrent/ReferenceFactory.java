// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

/**
 * Reference factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ReferenceFactory<T extends ReferenceCounted<?>> {

  /**
   * Creates a new reference.
   *
   * @param manager The reference manager.
   * @return The created reference.
   */
  T createReference(ReferenceManager<T> manager);

}
