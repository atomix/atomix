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
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.util.Managed;

/**
 * Copycat resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Resource<T extends Resource<?>> extends Managed<T> {

  /**
   * Returns the resource name.
   *
   * @return The resource name.
   */
  String name();

  /**
   * Returns the resource cluster.
   *
   * @return The resource cluster.
   */
  Cluster cluster();

  /**
   * Resource builder.
   *
   * @param <T> The resource builder type.
   * @param <U> The resource type.
   */
  static abstract class Builder<T extends Builder<T, U>, U extends Resource<?>> {
    private final ResourceConfig config;

    protected Builder(ResourceConfig config) {
      this.config = config;
    }

    /**
     * Sets the resource name.
     *
     * @param name The resource name.
     * @return The resource builder.
     */
    @SuppressWarnings("unchecked")
    public T withName(String name) {
      config.setName(name);
      return (T) this;
    }

    /**
     * Sets the resource cluster.
     *
     * @param cluster The resource cluster.
     * @return The resource builder.
     */
    @SuppressWarnings("unchecked")
    public T withCluster(ManagedCluster cluster) {
      config.setCluster(cluster);
      return (T) this;
    }

    /**
     * Builds the resource.
     *
     * @return The built resource.
     */
    public abstract U build();
  }

}
