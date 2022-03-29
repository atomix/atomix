// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.protocol;

import io.atomix.utils.event.EventListener;

/**
 * Node discovery event listener.
 */
public interface GroupMembershipEventListener extends EventListener<GroupMembershipEvent> {
}
