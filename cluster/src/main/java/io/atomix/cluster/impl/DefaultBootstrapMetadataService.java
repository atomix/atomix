/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import io.atomix.cluster.BootstrapConfig;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ManagedBootstrapMetadataService;
import io.atomix.cluster.Node;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Default bootstrap metadata service.
 */
public class DefaultBootstrapMetadataService implements ManagedBootstrapMetadataService {
  private final ClusterMetadata metadata;
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultBootstrapMetadataService(BootstrapConfig config) {
    this(new ClusterMetadata(config.getNodes().stream().map(Node::new).collect(Collectors.toList())));
  }

  public DefaultBootstrapMetadataService(ClusterMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public ClusterMetadata getMetadata() {
    return metadata;
  }

  @Override
  public void addListener(ClusterMetadataEventListener listener) {

  }

  @Override
  public void removeListener(ClusterMetadataEventListener listener) {

  }

  @Override
  public CompletableFuture<ClusterMetadataService> start() {
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("metadata", metadata)
        .toString();
  }
}
