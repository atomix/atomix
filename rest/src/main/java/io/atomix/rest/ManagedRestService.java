// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest;

import io.atomix.utils.Managed;

/**
 * Managed REST service.
 */
public interface ManagedRestService extends RestService, Managed<RestService> {
}
