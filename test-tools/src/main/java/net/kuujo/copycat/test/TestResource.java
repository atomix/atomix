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
package net.kuujo.copycat.test;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.resource.internal.AbstractResource;
import net.kuujo.copycat.resource.internal.ResourceContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Test resource implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestResource extends AbstractResource<TestResource> {

  public TestResource(ResourceContext context) {
    super(context);
  }

  @Override
  public CompletableFuture<TestResource> open() {
    return runStartupTasks().thenCompose(v -> context.open()).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close().thenCompose(v -> runShutdownTasks());
  }

  /**
   * Test resource configuration.
   */
  public static class Config extends ResourceConfig<Config> {
    public Config() {
    }

    public Config(Map<String, Object> config, String... resources) {
      super(config, resources);
    }

    public Config(Config config) {
      super(config);
    }

    public Config(String... resources) {
      super(resources);
    }

    @Override
    public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
      return new CoordinatedResourceConfig(super.toMap())
        .withElectionTimeout(getElectionTimeout())
        .withHeartbeatInterval(getHeartbeatInterval())
        .withResourceType(TestResource.class)
        .withLog(getLog())
        .withSerializer(getSerializer())
        .withExecutor(getExecutor())
        .withResourceConfig(this)
        .withReplicas(getReplicas().isEmpty() ? cluster.getMembers() : getReplicas());
    }

  }

}
