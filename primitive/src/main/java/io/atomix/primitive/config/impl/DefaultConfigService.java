// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.config.impl;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;

import java.util.Collection;
import java.util.Map;

/**
 * Default configuration service.
 */
public class DefaultConfigService implements ConfigService {
  private final Map<String, PrimitiveConfig> defaultConfigs = Maps.newConcurrentMap();
  private final Map<String, PrimitiveConfig> configs = Maps.newConcurrentMap();

  public DefaultConfigService(Collection<PrimitiveConfig> defaultConfigs, Collection<PrimitiveConfig> configs) {
    defaultConfigs.forEach(config -> this.defaultConfigs.put(((PrimitiveType) config.getType()).name(), config));
    configs.forEach(config -> this.configs.put(config.getName(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends PrimitiveConfig<C>> C getConfig(String primitiveName, PrimitiveType primitiveType) {
    C config = (C) configs.get(primitiveName);
    if (config != null) {
      return config;
    }
    if (primitiveType == null) {
      return null;
    }
    config = (C) defaultConfigs.get(primitiveType.name());
    if (config != null) {
      return config;
    }
    return (C) primitiveType.newConfig();
  }
}
