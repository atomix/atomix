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
package net.kuujo.copycat.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import net.kuujo.copycat.util.internal.Assert;

import java.util.*;

/**
 * Base configuration for configurable types.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractConfigurable implements Configurable {
  protected Config config;

  protected AbstractConfigurable() {
    this(new HashMap<>(128));
  }

  protected AbstractConfigurable(Map<String, Object> map, String... resources) {
    Config config = ConfigFactory.parseMap(map);
    if (resources != null) {
      for (String resource : resources) {
        config = config.withFallback(ConfigFactory.load(resource));
      }
    }
    this.config = config.resolve();
  }

  protected AbstractConfigurable(String... resources) {
    Assert.isNotNull(resources, "resources");
    Config config = null;
    for (String resource : resources) {
      if (config == null) {
        config = ConfigFactory.load(resource, ConfigParseOptions.defaults().setAllowMissing(true), ConfigResolveOptions.noSystem());
      } else {
        config = config.withFallback(ConfigFactory.load(resource, ConfigParseOptions.defaults().setAllowMissing(true), ConfigResolveOptions.noSystem()));
      }

      // Support namespaced configuration resources, e.g. foo.bar.baz, foo.bar, foo
      int index = resource.length();
      while ((index = resource.lastIndexOf('.', index - 1)) != -1) {
        config = config.withFallback(ConfigFactory.load(resource.substring(0, index), ConfigParseOptions.defaults().setAllowMissing(true), ConfigResolveOptions.noSystem()));
      }
    }
    this.config = config.resolve(ConfigResolveOptions.noSystem());
  }

  protected AbstractConfigurable(Configurable config) {
    this(config.toMap());
  }

  /**
   * Adds new resources to a resources array.
   */
  protected static String[] addResources(String[] resources, String... newResources) {
    List<String> results = resources != null ? new ArrayList<>(Arrays.asList(resources)) : new ArrayList<>(1);
    if (newResources != null) {
      results.addAll(Arrays.asList(newResources));
    }
    return results.toArray(new String[results.size()]);
  }

  @Override
  public Configurable copy() {
    try {
      Configurable copy = getClass().newInstance();
      copy.configure(toMap());
      return copy;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to create configuration copy", e);
    }
  }

  @Override
  public void configure(Map<String, Object> config) {
    this.config = ConfigFactory.parseMap(config);
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> config = new HashMap<>(this.config.root().unwrapped());
    config.put("class", getClass().getName());
    return config;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Map) {
      return config.equals(object);
    } else if (object instanceof Configurable) {
      return ((Configurable) object).toMap().equals(config);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 37 * config.hashCode();
  }

  @Override
  public String toString() {
    return String.format("Config%s", config);
  }

}
