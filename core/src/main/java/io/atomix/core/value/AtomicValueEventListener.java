// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.value;

import io.atomix.utils.event.EventListener;

/**
 * Listener to be notified about updates to a AtomicValue.
 */
public interface AtomicValueEventListener<V> extends EventListener<AtomicValueEvent<V>> {
}
