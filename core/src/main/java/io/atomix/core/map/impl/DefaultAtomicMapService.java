// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import io.atomix.core.map.AtomicMapType;

/**
 * Default atomic map service.
 */
public class DefaultAtomicMapService extends AbstractAtomicMapService {
  public DefaultAtomicMapService() {
    super(AtomicMapType.instance());
  }
}
