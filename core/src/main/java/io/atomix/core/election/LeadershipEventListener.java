// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election;

import io.atomix.utils.event.EventListener;

/**
 * Entity capable of receiving leader elector events.
 */
public interface LeadershipEventListener<T> extends EventListener<LeadershipEvent<T>> {
}
