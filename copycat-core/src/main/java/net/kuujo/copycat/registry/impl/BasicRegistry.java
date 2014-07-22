/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.registry.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.kuujo.copycat.registry.Registry;

/**
 * Basic registry implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BasicRegistry implements Registry {
  private final Map<String, Object> registry = new HashMap<>();

  @Override
  public Set<String> keys() {
    return registry.keySet();
  }

  @Override
  public Registry bind(String key, Object value) {
    registry.put(key, value);
    return this;
  }

  @Override
  public Registry unbind(String key) {
    registry.remove(key);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T lookup(String key) {
    return (T) registry.get(key);
  }

  @Override
  public <T> T lookup(String key, Class<T> type) {
    return type.cast(registry.get(key));
  }

}
