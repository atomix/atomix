// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AtomicNavigableMapType;

/**
 * Default atomic tree map service.
 */
public class DefaultAtomicNavigableMapService<K extends Comparable<K>> extends AbstractAtomicNavigableMapService<K> {
  public DefaultAtomicNavigableMapService() {
    super(AtomicNavigableMapType.instance());
  }
}
