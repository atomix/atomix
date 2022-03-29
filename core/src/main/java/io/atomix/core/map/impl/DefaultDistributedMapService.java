// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.DistributedMapType;

/**
 * Default distributed map service.
 */
public class DefaultDistributedMapService extends AbstractAtomicMapService {
  public DefaultDistributedMapService() {
    super(DistributedMapType.instance());
  }
}
