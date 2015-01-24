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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncSet;

import java.util.Map;

/**
 * Asynchronous set configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncSetConfig extends AsyncCollectionConfig<AsyncSetConfig> {

  public AsyncSetConfig() {
  }

  public AsyncSetConfig(Map<String, Object> config) {
    super(config);
  }

  protected AsyncSetConfig(AsyncSetConfig config) {
    super(config);
  }

  @Override
  public AsyncSetConfig copy() {
    return new AsyncSetConfig(this);
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    return new StateLogConfig(toMap())
      .resolve(cluster)
      .withResourceFactory(DefaultAsyncSet::new);
  }

}
