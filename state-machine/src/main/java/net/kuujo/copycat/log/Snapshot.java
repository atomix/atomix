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
package net.kuujo.copycat.log;

import net.kuujo.copycat.cluster.Cluster;

import java.util.Map;

/**
 * Log snapshot.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Snapshot {

  /**
   * Returns a new snapshot builder.
   *
   * @return A new snapshot builder.
   */
  static Builder builder() {
    return null;
  }

  /**
   * Returns the snapshot term.
   *
   * @return The snapshot term.
   */
  long term();

  /**
   * Returns the snapshot cluster configuration.
   *
   * @return The snapshot cluster configuration.
   */
  Cluster cluster();

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  Map<String, Object> data();

  /**
   * Snapshot builder.
   */
  static interface Builder {

    /**
     * Sets the snapshot term.
     *
     * @param term The snapshot term.
     * @return The snapshot builder.
     */
    Builder withTerm(long term);

    /**
     * Sets the snapshot cluster configuration.
     *
     * @param cluster The snapshot cluster configuration.
     * @return The snapshot builder.
     */
    Builder withCluster(Cluster cluster);

    /**
     * Sets the snapshot data.
     *
     * @param data The snapshot data.
     * @return The snapshot builder.
     */
    Builder withData(Map<String, Object> data);

    /**
     * Builds the snapshot.
     *
     * @return The built snapshot.
     */
    Snapshot build();

  }

}
