// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.set;

import io.atomix.utils.event.EventListener;

/**
 * Set protocol event listener.
 */
public interface SetDelegateEventListener<E> extends EventListener<SetDelegateEvent<E>> {
}
