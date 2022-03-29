// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

/**
 * Reference manager. Manages {@link ReferenceCounted} objects.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ReferenceManager<T> {

  /**
   * Releases the given reference.
   * <p>
   * This method should be called with a {@link ReferenceCounted} object that contains no
   * additional references. This allows, for instance, pools to recycle dereferenced objects.
   *
   * @param reference The reference to release.
   */
  void release(T reference);

}
