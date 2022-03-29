// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.DistributedNavigableMapType;

/**
 * Default distributed tree map service.
 */
public class DefaultDistributedNavigableMapService<K extends Comparable<K>> extends AbstractAtomicNavigableMapService<K> {
  public DefaultDistributedNavigableMapService() {
    super(DistributedNavigableMapType.instance());
  }
}
