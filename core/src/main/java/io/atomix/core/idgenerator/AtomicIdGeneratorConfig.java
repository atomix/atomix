// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;

/**
 * ID generator configuration.
 */
public class AtomicIdGeneratorConfig extends PrimitiveConfig<AtomicIdGeneratorConfig> {
  @Override
  public PrimitiveType getType() {
    return AtomicIdGeneratorType.instance();
  }
}
