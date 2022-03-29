// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.discovery;

import io.atomix.utils.event.EventListener;

/**
 * Node discovery event listener.
 */
public interface NodeDiscoveryEventListener extends EventListener<NodeDiscoveryEvent> {
}
