// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap.impl;

import io.atomix.core.multimap.AtomicMultimapType;

/**
 * Default atomic multimap service.
 */
public class DefaultAtomicMultimapService extends AbstractAtomicMultimapService {
  public DefaultAtomicMultimapService() {
    super(AtomicMultimapType.instance());
  }
}
