/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.utils.config;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import io.atomix.core.AtomixRegistry;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.config.TypedConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Polymorphic configuration mapper.
 */
public class PolymorphicConfigMapper extends ConfigMapper {
  private final AtomixRegistry registry;
  private final Map<Class, PolymorphicTypeMapper> polymorphicTypes = Maps.newHashMap();

  public PolymorphicConfigMapper(ClassLoader classLoader, AtomixRegistry registry) {
    this(classLoader, registry, Collections.emptyList());
  }

  public PolymorphicConfigMapper(ClassLoader classLoader, AtomixRegistry registry, PolymorphicTypeMapper... polymorphicTypeMappers) {
    this(classLoader, registry, Arrays.asList(polymorphicTypeMappers));
  }

  public PolymorphicConfigMapper(ClassLoader classLoader, AtomixRegistry registry, Collection<PolymorphicTypeMapper> polymorphicTypeMappers) {
    super(classLoader);
    this.registry = checkNotNull(registry);
    polymorphicTypeMappers.forEach(mapper -> this.polymorphicTypes.put(mapper.getTypedClass(), mapper));
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <T> T newInstance(Config config, Class<T> clazz) {
    T instance;

    // If the class is a polymorphic type, look up the type mapper and get the concrete type.
    if (isPolymorphicType(clazz)) {
      PolymorphicTypeMapper typeMapper = polymorphicTypes.get(clazz);
      if (typeMapper == null) {
        throw new ConfigurationException("Cannot instantiate abstract type " + clazz.getName());
      }

      String typeName = config.getString(typeMapper.getTypePath());
      Class<? extends TypedConfig<?, ?>> concreteClass = typeMapper.getConcreteClass(registry, typeName);
      try {
        instance = (T) concreteClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException(concreteClass.getName() + " needs a public no-args constructor to be used as a bean", e);
      }
    } else {
      try {
        instance = clazz.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException(clazz.getName() + " needs a public no-args constructor to be used as a bean", e);
      }
    }
    return instance;
  }

  @Override
  protected void checkRemainingProperties(Set<String> propertyNames, String path, Class<?> clazz) {
    Set<String> cleanNames = propertyNames
        .stream()
        .filter(propertyName -> !isPolymorphicType(clazz) || !propertyName.equals(polymorphicTypes.get(clazz).getTypePath()))
        .map(propertyName -> toPath(path, propertyName))
        .collect(Collectors.toSet());
    if (!cleanNames.isEmpty()) {
      throw new ConfigurationException("Unknown properties present in configuration: " + Joiner.on(", ").join(cleanNames));
    }
  }

  /**
   * Returns a boolean indicating whether the given class is a polymorphic type.
   */
  private boolean isPolymorphicType(Class<?> clazz) {
    return polymorphicTypes.containsKey(clazz);
  }
}
