// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.collection;

import io.atomix.utils.event.EventListener;

/**
 * Listener to be notified about updates to a DistributedSet.
 */
public interface CollectionEventListener<E> extends EventListener<CollectionEvent<E>> {
}
