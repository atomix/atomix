// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.cluster;

import io.atomix.utils.event.EventListener;

/**
 * Raft cluster event listener.
 */
@FunctionalInterface
public interface RaftClusterEventListener extends EventListener<RaftClusterEvent> {
}
