/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.task;

import io.atomix.catalyst.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface TaskProducer<T> extends AutoCloseable {

  /**
   * Task producer options.
   */
  class Options {
    private RoutingStrategy routingStrategy = RoutingStrategy.ALL;
    private FailoverStrategy failoverStrategy = FailoverStrategy.FAIL;

    /**
     * Sets the producer routing strategy.
     *
     * @param routingStrategy The routing strategy.
     * @return The producer options.
     */
    public Options withRoutingStrategy(RoutingStrategy routingStrategy) {
      this.routingStrategy = Assert.notNull(routingStrategy, "routingStrategy");
      return this;
    }

    /**
     * Returns the routing strategy.
     *
     * @return The routing strategy.
     */
    public RoutingStrategy getRoutingStrategy() {
      return routingStrategy;
    }

    /**
     * Sets the producer failover strategy.
     *
     * @param failoverStrategy The producer failover strategy.
     * @return The producer options.
     */
    public Options withFailoverStrategy(FailoverStrategy failoverStrategy) {
      this.failoverStrategy = Assert.notNull(failoverStrategy, "failoverStrategy");
      return this;
    }

    /**
     * Returns the failover strategy.
     *
     * @return The failover strategy.
     */
    public FailoverStrategy getFailoverStrategy() {
      return failoverStrategy;
    }
  }

  CompletableFuture<Void> submit(T task);

  @Override
  default void close() {
  }

}
