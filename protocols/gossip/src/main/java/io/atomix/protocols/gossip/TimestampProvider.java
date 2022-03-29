// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.utils.time.Timestamp;

/**
 * Timestamp provider.
 */
@Beta
@FunctionalInterface
public interface TimestampProvider<E> {

  /**
   * Returns the timestamp for the given entry.
   *
   * @param entry the entry for which to return the timestamp
   * @return the timestamp for the given entry
   */
  Timestamp get(E entry);

}
