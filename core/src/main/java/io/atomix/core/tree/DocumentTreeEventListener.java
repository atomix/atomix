// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree;

import io.atomix.utils.event.EventListener;

/**
 * A listener for {@link DocumentTreeEvent}.
 *
 * @param <V> document tree node value type
 */
public interface DocumentTreeEventListener<V> extends EventListener<DocumentTreeEvent<V>> {
}
