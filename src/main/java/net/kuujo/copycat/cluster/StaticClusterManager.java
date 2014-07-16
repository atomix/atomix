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
package net.kuujo.copycat.cluster;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * Static cluster manager implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StaticClusterManager implements ClusterManager {
  private final ClusterConfig config;

  public StaticClusterManager() {
    config = new StaticClusterConfig();
  }

  public StaticClusterManager(ClusterConfig config) {
    this.config = config;
  }

  @Override
  public ClusterConfig config() {
    return config;
  }

  @Override
  public ClusterManager start() {
    return start(null);
  }

  @Override
  public ClusterManager start(Handler<AsyncResult<ClusterConfig>> doneHandler) {
    new DefaultFutureResult<ClusterConfig>(config).setHandler(doneHandler);
    return this;
  }

  @Override
  public void stop() {
    stop(null);
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

}
