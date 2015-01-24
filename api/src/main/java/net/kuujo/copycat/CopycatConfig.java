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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.CoordinatorConfig;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig extends AbstractConfigurable {
  public static final String COPYCAT_NAME = "name";
  public static final String COPYCAT_DEFAULT_SERIALIZER = "serializer";
  public static final String COPYCAT_DEFAULT_EXECUTOR = "executor";
  public static final String COPYCAT_CLUSTER = "cluster";

  private static final String DEFAULT_COPYCAT_NAME = "copycat";
  private static final String DEFAULT_COPYCAT_SERIALIZER = KryoSerializer.class.getName();
  private final Executor DEFAULT_COPYCAT_EXECUTOR = Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-%d"));

  public CopycatConfig() {
    super();
  }

  public CopycatConfig(Map<String, Object> config) {
    super(config);
  }

  private CopycatConfig(CopycatConfig config) {
    super(config);
  }

  @Override
  public CopycatConfig copy() {
    return new CopycatConfig(this);
  }

  /**
   * Sets the Copycat instance name.
   *
   * @param name The Copycat instance name.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public void setName(String name) {
    put(COPYCAT_NAME, Assert.isNotNull(name, "name"));
  }

  /**
   * Returns the Copycat instance name.
   *
   * @return The Copycat instance name.
   */
  public String getName() {
    return get(COPYCAT_NAME, DEFAULT_COPYCAT_NAME);
  }

  /**
   * Sets the Copycat instance name, returning the configuration for method chaining.
   *
   * @param name The Copycat instance name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public CopycatConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the Copycat cluster configuration.
   *
   * @param config The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig config) {
    put(COPYCAT_CLUSTER, Assert.isNotNull(config, "config"));
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return get(COPYCAT_CLUSTER, key -> new ClusterConfig());
  }

  /**
   * Sets the Copycat cluster configuration, returning the Copycat configuration for method chaining.
   *
   * @param config The Copycat cluster configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public CopycatConfig withClusterConfig(ClusterConfig config) {
    setClusterConfig(config);
    return this;
  }

  /**
   * Sets the default resource entry serializer class name.
   *
   * @param serializer The default resource entry serializer class name.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setDefaultSerializer(String serializer) {
    put(COPYCAT_DEFAULT_SERIALIZER, Assert.isNotNull(serializer, "serializer"));
  }

  /**
   * Sets the default resource entry serializer.
   *
   * @param serializer The default resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setDefaultSerializer(Class<? extends Serializer> serializer) {
    put(COPYCAT_DEFAULT_SERIALIZER, Assert.isNotNull(serializer, "serializer"));
  }

  /**
   * Sets the default resource entry serializer.
   *
   * @param serializer The default resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setDefaultSerializer(Serializer serializer) {
    put(COPYCAT_DEFAULT_SERIALIZER, Assert.isNotNull(serializer, "serializer"));
  }

  /**
   * Returns the default resource entry serializer.
   *
   * @return The default resource entry serializer.
   * @throws net.kuujo.copycat.ConfigurationException If the resource serializer configuration is malformed
   */
  @SuppressWarnings("unchecked")
  public Serializer getDefaultSerializer() {
    Object serializer = get(COPYCAT_DEFAULT_SERIALIZER, DEFAULT_COPYCAT_SERIALIZER);
    if (serializer instanceof Serializer) {
      return (Serializer) serializer;
    } else if (serializer instanceof Class) {
      try {
        return ((Class<? extends Serializer>) serializer).newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException("Failed to instantiate serializer", e);
      }
    } else if (serializer instanceof String) {
      try {
        return ((Class<? extends Serializer>) Class.forName(serializer.toString())).newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ConfigurationException("Failed to instantiate serializer", e);
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("Failed to locate serializer class", e);
      }
    }
    throw new IllegalStateException("Invalid default serializer configuration");
  }

  /**
   * Sets the default resource entry serializer class name, returning the configuration for method chaining.
   *
   * @param serializer The default resource entry serializer class name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CopycatConfig withSerializer(String serializer) {
    setDefaultSerializer(serializer);
    return this;
  }

  /**
   * Sets the default resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The default resource entry serializer.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CopycatConfig withDefaultSerializer(Class<? extends Serializer> serializer) {
    setDefaultSerializer(serializer);
    return this;
  }

  /**
   * Sets the default resource entry serializer, returning the configuration for method chaining.
   *
   * @param serializer The default resource entry serializer.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CopycatConfig withDefaultSerializer(Serializer serializer) {
    setDefaultSerializer(serializer);
    return this;
  }

  /**
   * Sets the Copycat executor.
   *
   * @param executor The Copycat executor.
   */
  public void setDefaultExecutor(Executor executor) {
    put(COPYCAT_DEFAULT_EXECUTOR, executor);
  }

  /**
   * Returns the Copycat executor.
   *
   * @return The Copycat executor or {@code null} if no executor was specified.
   */
  public Executor getDefaultExecutor() {
    return get(COPYCAT_DEFAULT_EXECUTOR, DEFAULT_COPYCAT_EXECUTOR);
  }

  /**
   * Sets the Copycat executor, returning the configuration for method chaining.
   *
   * @param executor The Copycat executor.
   * @return The Copycat configuration.
   */
  public CopycatConfig withDefaultExecutor(Executor executor) {
    setDefaultExecutor(executor);
    return this;
  }

  /**
   * Resolves the Copycat configuration to a coordinator configuration.
   *
   * @return A coordinator configuration for this Copycat configuration.
   */
  @SuppressWarnings("rawtypes")
  public CoordinatorConfig resolve() {
    return new CoordinatorConfig()
      .withExecutor(getDefaultExecutor())
      .withClusterConfig(getClusterConfig());
  }

}
