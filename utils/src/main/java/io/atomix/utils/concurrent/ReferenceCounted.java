// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

/**
 * Reference counting interface.
 * <p>
 * Types that implement {@code ReferenceCounted} can be counted for references and thus used to clean up resources once
 * a given instance of an object is no longer in use.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ReferenceCounted<T> extends AutoCloseable {

  /**
   * Acquires a reference.
   *
   * @return The acquired reference.
   */
  T acquire();

  /**
   * Releases a reference.
   *
   * @return Indicates whether all references to the object have been released.
   */
  boolean release();

  /**
   * Returns the number of open references.
   *
   * @return The number of open references.
   */
  int references();

  /**
   * Defines an exception free close implementation.
   */
  @Override
  void close();

}
