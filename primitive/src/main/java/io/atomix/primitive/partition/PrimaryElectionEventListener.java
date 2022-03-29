// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.utils.event.EventListener;

/**
 * Primary election event listener.
 */
public interface PrimaryElectionEventListener extends EventListener<PrimaryElectionEvent> {
}
