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
package net.kuujo.copycat.test;

import net.kuujo.copycat.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.LocalProtocol;

import java.util.Arrays;
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
  private final List<T> activeResources;
  private final List<T> passiveResources;

  private TestCluster(List<T> activeResources, List<T> passiveResources) {
    this.activeResources = activeResources;
    this.passiveResources = passiveResources;
  }

  /**
   * Creates a test cluster for the given resource factory.
   */
  @SuppressWarnings("all")
  public static <T extends Resource<T>> TestCluster<T> of(BiFunction<String, ClusterConfig, T> factory) {
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new LocalProtocol());
    for (int i = 1; i <= 3; i++) {
      cluster.addMember(String.format("local://test%d", i));
    }
    List<T> activeResources = cluster.getMembers().stream().collect(Collectors.mapping(uri -> factory.apply(uri, cluster), Collectors.toList()));
    List<T> passiveResources = Arrays.asList("local://test4", "local://test5").stream().collect(Collectors.mapping(uri -> factory.apply(uri, cluster), Collectors.toList()));
    return new TestCluster<T>(activeResources, passiveResources);
  }

  /**
   * Returns a list of active cluster resources.
   *
   * @return A list of active cluster resources.
   */
  public List<T> activeResources() {
    return activeResources;
  }

  /**
   * Returns a list of passive cluster resources.
   *
   * @return A list of passive cluster resources.
   */
  public List<T> passiveResources() {
    return passiveResources;
  }

  /**
   * Opens all resources.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> open() {
    CompletableFuture<Void>[] futures = new CompletableFuture[activeResources.size() + passiveResources.size()];
    int i = 0;
    for (T resource : activeResources) {
      futures[i++] = resource.open().thenRun(() -> System.out.println(resource.cluster().member().uri() + " started successfully!")).thenApply(v -> null);
    }
    for (T resource : passiveResources) {
      futures[i++] = resource.open().thenRun(() -> System.out.println(resource.cluster().member().uri() + " started successfully!")).thenApply(v -> null);
    }
    return CompletableFuture.allOf(futures);
  }

  /**
   * Closes all resources.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    CompletableFuture<Void>[] futures = new CompletableFuture[activeResources.size() + passiveResources.size()];
    int i = 0;
    for (T resource : passiveResources) {
      futures[i++] = resource.close();
    }
    for (T resource : activeResources) {
      futures[i++] = resource.close();
    }
    return CompletableFuture.allOf(futures);
  }
}
