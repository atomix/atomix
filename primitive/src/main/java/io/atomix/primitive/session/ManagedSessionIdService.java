// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session;

import io.atomix.utils.Managed;

/**
 * Managed session ID service.
 */
public interface ManagedSessionIdService extends SessionIdService, Managed<SessionIdService> {
}
