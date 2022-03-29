// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging;

import io.atomix.utils.Managed;

/**
 * Managed messaging service.
 */
public interface ManagedMessagingService extends MessagingService, Managed<MessagingService> {
}
