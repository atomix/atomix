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

import net.kuujo.copycat.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.LocalProtocol;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Test cluster utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestCluster<T extends Resource<T>> {
  private final List<T> resources;

  private TestCluster(List<T> resources) {
    this.resources = resources;
  }

  /**
   * Creates a test cluster for the given resource factory.
   */
  public static <T extends Resource<T>> TestCluster<T> of(BiFunction<String, ClusterConfig, T> factory) {
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new LocalProtocol());
    for (int i = 1; i <= 5; i++) {
      cluster.addMember(String.format("local://%d", i));
    }
    return new TestCluster<T>(cluster.getMembers().stream().collect(Collectors.mapping(uri -> factory.apply(uri, cluster), Collectors
      .toList())));
  }

  /**
   * Returns a list of cluster resources.
   */
  public List<T> resources() {
    return resources;
  }

  /**
   * Opens all resources.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    CompletableFuture<Void>[] futures = new CompletableFuture[resources.size()];
    for (int i = 0; i < resources.size(); i++) {
      T resource = resources.get(i);
      futures[i] = resources.get(i).open().thenRun(() -> System.out.println(resource.cluster()
        .member()
        .uri() + " started successfully!")).thenApply(v -> null);
    }
    return CompletableFuture.allOf(futures);
  }

  /**
   * Closes all resources.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    CompletableFuture<Void>[] futures = new CompletableFuture[resources.size()];
    for (int i = 0; i < resources.size(); i++) {
      futures[i] = resources.get(i).close();
    }
    return CompletableFuture.allOf(futures);
  }
}
