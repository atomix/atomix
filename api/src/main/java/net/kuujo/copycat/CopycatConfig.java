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

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.Map;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig extends AbstractConfigurable {
  public static final String COPYCAT_NAME = "name";
  public static final String COPYCAT_DEFAULT_SERIALIZER = "serializer";
  public static final String COPYCAT_CLUSTER = "cluster";

  private static final String DEFAULT_CONFIGURATION = "copycat-default";
  private static final String CONFIGURATION = "copycat";

  public CopycatConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public CopycatConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public CopycatConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
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
    this.config = config.withValue(COPYCAT_NAME, ConfigValueFactory.fromAnyRef(Assert.isNotNull(name, "name")));
  }

  /**
   * Returns the Copycat instance name.
   *
   * @return The Copycat instance name.
   */
  public String getName() {
    return config.getString(COPYCAT_NAME);
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
   * @param cluster The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig cluster) {
    this.config = config.withValue(COPYCAT_CLUSTER, ConfigValueFactory.fromMap(Assert.isNotNull(cluster, "cluster").toMap()));
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return Configurable.load(config.getObject(COPYCAT_CLUSTER).unwrapped());
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
    try {
      setDefaultSerializer((Serializer) Class.forName(serializer).newInstance());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate serializer", e);
    }
  }

  /**
   * Sets the default resource entry serializer.
   *
   * @param serializer The default resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setDefaultSerializer(Class<? extends Serializer> serializer) {
    try {
      setDefaultSerializer((Serializer) serializer.newInstance());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException("Failed to instantiate serializer", e);
    }
  }

  /**
   * Sets the default resource entry serializer.
   *
   * @param serializer The default resource entry serializer.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
  public void setDefaultSerializer(Serializer serializer) {
    this.config = config.withValue(COPYCAT_DEFAULT_SERIALIZER, ConfigValueFactory.fromMap(Assert.isNotNull(serializer, "serializer").toMap()));
  }

  /**
   * Returns the default resource entry serializer.
   *
   * @return The default resource entry serializer.
   * @throws net.kuujo.copycat.util.ConfigurationException If the resource serializer configuration is malformed
   */
  public Serializer getDefaultSerializer() {
    return Configurable.load(config.getObject(COPYCAT_DEFAULT_SERIALIZER).unwrapped());
  }

  /**
   * Sets the default resource entry serializer class name, returning the configuration for method chaining.
   *
   * @param serializer The default resource entry serializer class name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the serializer is {@code null}
   */
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
  public CopycatConfig withDefaultSerializer(Serializer serializer) {
    setDefaultSerializer(serializer);
    return this;
  }

  @Override
  public String toString() {
    return String.format("%s[%s]", getClass().getSimpleName(), config.root().unwrapped());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof CopycatConfig && ((CopycatConfig) object).config.equals(config);
  }

  @Override
  public int hashCode() {
    return 17 * config.root().unwrapped().hashCode();
  }

}
