/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.utils;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Service utilities.
 */
public final class Services {
  private static final Map<Class, Object> services = Maps.newConcurrentMap();

  /**
   * Loads the service for the given service class.
   *
   * @param serviceClass the service class for which to load the service
   * @param <T> the service type
   * @return the registered service of the given type
   */
  @SuppressWarnings("unchecked")
  public static <T> T load(Class<T> serviceClass) {
    return (T) services.computeIfAbsent(serviceClass, s -> ServiceLoader.load(serviceClass).iterator().next());
  }

  /**
   * Loads all services for the given service class.
   *
   * @param serviceClass the service class for which to load the services
   * @param <T> the service type
   * @return the registered services of the given type
   */
  public static <T> Collection<T> loadAll(Class<T> serviceClass) {
    List<T> services = new ArrayList<>();
    Iterator<T> iterator = ServiceLoader.load(serviceClass).iterator();
    while (iterator.hasNext()) {
      services.add(iterator.next());
    }
    return services;
  }

  private Services() {
  }
}
