// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.value;

import io.atomix.utils.event.EventListener;

/**
 * Value protocol event listener.
 */
public interface ValueDelegateEventListener<E> extends EventListener<ValueDelegateEvent<E>> {
}
