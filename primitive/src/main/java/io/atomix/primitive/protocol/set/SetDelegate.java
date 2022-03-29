// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.set;

import com.google.common.annotations.Beta;

import java.util.Set;

/**
 * Gossip-based set service.
 */
@Beta
public interface SetDelegate<E> extends Set<E> {

  /**
   * Adds the specified listener to the set which will be notified whenever the entries in the set are changed.
   *
   * @param listener listener to register for events
   */
  void addListener(SetDelegateEventListener<E> listener);

  /**
   * Removes the specified listener from the set such that it will no longer receive change notifications.
   *
   * @param listener listener to deregister for events
   */
  void removeListener(SetDelegateEventListener<E> listener);

  /**
   * Closes the set.
   */
  void close();

}
