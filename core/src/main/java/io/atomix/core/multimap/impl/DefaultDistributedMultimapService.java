// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap.impl;

import io.atomix.core.multimap.DistributedMultimapType;

/**
 * Default distributed multimap service.
 */
public class DefaultDistributedMultimapService extends AbstractAtomicMultimapService {
  public DefaultDistributedMultimapService() {
    super(DistributedMultimapType.instance());
  }
}
