/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.util.internal;

import net.kuujo.copycat.spi.ConfigResolver;

import java.util.*;

/**
 * Configuration utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Config {
  private static final Map<Class, List<?>> resolvers = new HashMap<>();

  /**
   * Loads a set of ordered resolvers for the given resolver type.
   *
   * @param type The resolver type.
   * @param <T> The resolver type.
   * @return A list of resolvers ordered by weight.
   */
  @SuppressWarnings("unchecked")
  public static <T extends ConfigResolver<?>> T[] loadOrderedResolvers(Class<T> type) {
    if (Config.resolvers.containsKey(type)) {
      return (T[]) Config.resolvers.get(type).toArray();
    }

    // Get a list of resolvers.
    List<T> resolvers = new ArrayList<>(10);
    ServiceLoader<T> loader = ServiceLoader.load(type);
    for (T resolver : loader) {
      resolvers.add(resolver);
    }

    // Sort the list of resolvers by weight.
    Collections.sort(resolvers, (a, b) -> {
      return a.weight() < b.weight() ? -1 : 1;
    });
    Config.resolvers.put(type, resolvers);
    return (T[]) resolvers.toArray();
  }

  /**
   * Resolves the configuration for the given value.
   *
   * @param value The value for which to resolve the configuration.
   * @param type The resolver type.
   * @param <T> The value type.
   * @param <U> The resolver type.
   * @return The resolved configuration value.
   */
  public static <T, U extends ConfigResolver<T>> T resolve(T value, Class<U> type) {
    for (U resolver : loadOrderedResolvers(type)) {
      resolver.resolve(value);
    }
    return value;
  }

}
