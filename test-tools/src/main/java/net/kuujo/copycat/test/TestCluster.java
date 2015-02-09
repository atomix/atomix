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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.resource.Resource;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Test cluster utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestCluster<T extends Resource<T>> {
  private static int id;
  private final List<T> activeResources;
  private final List<T> passiveResources;

  private TestCluster(List<T> activeResources, List<T> passiveResources) {
    this.activeResources = activeResources;
    this.passiveResources = passiveResources;
  }

  /**
   * Creates a new test cluster builder.
   */
  public static <T extends Resource<T>> Builder<T> builder() {
    return new Builder<>();
  }

  /**
   * Test cluster builder.
   */
  public static class Builder<T extends Resource<T>> {
    private int activeMembers = 3;
    private int passiveMembers = 2;
    private Function<Integer, String> uriFactory;
    private Function<Collection<String>, ClusterConfig> clusterFactory;
    private Function<ClusterConfig, T> resourceFactory;

    /**
     * Sets the number of active members for the cluster.
     */
    public Builder<T> withActiveMembers(int activeMembers) {
      this.activeMembers = activeMembers;
      return this;
    }

    /**
     * Sets the number of passive members for the cluster.
     */
    public Builder<T> withPassiveMembers(int passiveMembers) {
      this.passiveMembers = passiveMembers;
      return this;
    }

    /**
     * Sets the member URI factory.
     */
    public Builder<T> withUriFactory(Function<Integer, String> uriFactory) {
      this.uriFactory = uriFactory;
      return this;
    }

    /**
     * Sets the cluster configuration factory.
     */
    public Builder<T> withClusterFactory(Function<Collection<String>, ClusterConfig> clusterFactory) {
      this.clusterFactory = clusterFactory;
      return this;
    }

    /**
     * Sets the resource factory.
     */
    public Builder<T> withResourceFactory(Function<ClusterConfig, T> resourceFactory) {
      this.resourceFactory = resourceFactory;
      return this;
    }

    /**
     * Builds the test cluster.
     */
    public TestCluster<T> build() {
      LocalProtocol.reset();

      List<T> activeResources = new ArrayList<>(activeMembers);

      Set<String> members = new HashSet<>(activeMembers);
      int activeCount = activeMembers + id;
      while (id <= activeCount) {
        String uri = uriFactory.apply(id++);
        members.add(uri);
      }

      for (String member : members) {
        ClusterConfig cluster = clusterFactory.apply(members).withLocalMember(member);
        activeResources.add(resourceFactory.apply(cluster));
      }

      List<T> passiveResources = new ArrayList<>(passiveMembers);
      int passiveCount = passiveMembers + id;
      while (id <= passiveCount) {
        String member = uriFactory.apply(id++);
        ClusterConfig cluster = clusterFactory.apply(members).withLocalMember(member);
        passiveResources.add(resourceFactory.apply(cluster));
      }
      return new TestCluster<T>(activeResources, passiveResources);
    }
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
