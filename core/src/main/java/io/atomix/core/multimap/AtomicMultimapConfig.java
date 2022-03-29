// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.multimap;

import io.atomix.primitive.PrimitiveType;

/**
 * Consistent multimap configuration.
 */
public class AtomicMultimapConfig extends MultimapConfig<AtomicMultimapConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicMultimapType.instance();
  }
}
