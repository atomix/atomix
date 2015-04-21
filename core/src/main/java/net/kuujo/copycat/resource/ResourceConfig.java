/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;

/**
 * Resource configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceConfig extends Config {
  private String name;
  private Cluster cluster;

  /**
   * Sets the resource name.
   *
   * @param name The resource name.
   */
  protected void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the resource name.
   *
   * @return The resource name.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the resource cluster.
   *
   * @param cluster The resource cluster.
   */
  protected void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Returns the resource cluster.
   *
   * @return The resource cluster.
   */
  public Cluster getCluster() {
    return cluster;
  }

  @Override
  protected ResourceConfig resolve() {
    if (name == null)
      throw new ConfigurationException("name not configured");
    if (cluster == null)
      throw new ConfigurationException("cluster not configured");
    return this;
  }
}
