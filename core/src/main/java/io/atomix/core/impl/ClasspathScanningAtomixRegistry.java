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
package io.atomix.core.impl;

import io.atomix.core.AtomixRegistry;
import io.atomix.core.profile.Profile;
import io.atomix.core.profile.ProfileRegistry;
import io.atomix.core.profile.impl.DefaultProfileRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.impl.DefaultPartitionGroupTypeRegistry;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.protocol.impl.DefaultPrimitiveProtocolTypeRegistry;
import io.atomix.utils.ServiceException;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Atomix registry that scans the classpath for registered objects.
 */
public class ClasspathScanningAtomixRegistry implements AtomixRegistry {
  private final PartitionGroupTypeRegistry partitionGroupTypes;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimitiveProtocolTypeRegistry protocolTypes;
  private final ProfileRegistry profileTypes;

  public ClasspathScanningAtomixRegistry(ClassLoader classLoader) {
    final FastClasspathScanner classpathScanner = new FastClasspathScanner().addClassLoader(classLoader);

    final Map<String, PartitionGroup.Type> partitionGroupTypes = new ConcurrentHashMap<>();
    classpathScanner.matchClassesImplementing(PartitionGroup.Type.class, type -> {
      if (!Modifier.isAbstract(type.getModifiers()) && !Modifier.isPrivate(type.getModifiers())) {
        PartitionGroup.Type partitionGroupType = newInstance(type);
        partitionGroupTypes.put(partitionGroupType.name(), partitionGroupType);
      }
    });
    final Map<String, PrimitiveType> primitiveTypes = new ConcurrentHashMap<>();
    classpathScanner.matchClassesImplementing(PrimitiveType.class, type -> {
      if (!Modifier.isAbstract(type.getModifiers()) && !Modifier.isPrivate(type.getModifiers())) {
        PrimitiveType primitiveType = newInstance(type);
        primitiveTypes.put(primitiveType.name(), primitiveType);
      }
    });
    final Map<String, PrimitiveProtocol.Type> protocolTypes = new ConcurrentHashMap<>();
    classpathScanner.matchClassesImplementing(PrimitiveProtocol.Type.class, type -> {
      if (!Modifier.isAbstract(type.getModifiers()) && !Modifier.isPrivate(type.getModifiers())) {
        PrimitiveProtocol.Type protocolType = newInstance(type);
        protocolTypes.put(protocolType.name(), protocolType);
      }
    });
    final Map<String, Profile> profileTypes = new ConcurrentHashMap<>();
    classpathScanner.matchClassesImplementing(Profile.class, profile -> {
      if (!Modifier.isAbstract(profile.getModifiers()) && !Modifier.isPrivate(profile.getModifiers())) {
        Profile profileType = newInstance(profile);
        profileTypes.put(profileType.name(), profileType);
      }
    });
    classpathScanner.scan();

    this.partitionGroupTypes = new DefaultPartitionGroupTypeRegistry(partitionGroupTypes);
    this.primitiveTypes = new DefaultPrimitiveTypeRegistry(primitiveTypes);
    this.protocolTypes = new DefaultPrimitiveProtocolTypeRegistry(protocolTypes);
    this.profileTypes = new DefaultProfileRegistry(profileTypes);
  }

  /**
   * Instantiates the given type using a no-argument constructor.
   *
   * @param type the type to instantiate
   * @param <T>  the generic type
   * @return the instantiated object
   * @throws ServiceException if the type cannot be instantiated
   */
  @SuppressWarnings("unchecked")
  private static <T> T newInstance(Class<?> type) {
    try {
      return (T) type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ServiceException("Cannot instantiate service class " + type, e);
    }
  }

  @Override
  public PartitionGroupTypeRegistry partitionGroupTypes() {
    return partitionGroupTypes;
  }

  @Override
  public PrimitiveTypeRegistry primitiveTypes() {
    return primitiveTypes;
  }

  @Override
  public PrimitiveProtocolTypeRegistry protocolTypes() {
    return protocolTypes;
  }

  @Override
  public ProfileRegistry profiles() {
    return profileTypes;
  }
}
