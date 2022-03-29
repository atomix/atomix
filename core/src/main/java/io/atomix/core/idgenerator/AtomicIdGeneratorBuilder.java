// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.idgenerator;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Builder for AtomicIdGenerator.
 */
public abstract class AtomicIdGeneratorBuilder
    extends PrimitiveBuilder<AtomicIdGeneratorBuilder, AtomicIdGeneratorConfig, AtomicIdGenerator>
    implements ProxyCompatibleBuilder<AtomicIdGeneratorBuilder> {

  protected AtomicIdGeneratorBuilder(String name, AtomicIdGeneratorConfig config, PrimitiveManagementService managementService) {
    super(AtomicIdGeneratorType.instance(), name, config, managementService);
  }

  @Override
  public AtomicIdGeneratorBuilder withProtocol(ProxyProtocol protocol) {
    return withProtocol((PrimitiveProtocol) protocol);
  }
}
