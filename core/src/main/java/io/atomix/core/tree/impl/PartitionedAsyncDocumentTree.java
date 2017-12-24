/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.tree.impl;

import com.google.common.collect.Maps;

import io.atomix.core.tree.AsyncDocumentTree;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.tree.DocumentTreeListener;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Partitioned asynchronous document tree.
 */
public class PartitionedAsyncDocumentTree<V> implements AsyncDocumentTree<V> {

  private final String name;
  private final TreeMap<PartitionId, AsyncDocumentTree<V>> partitions = Maps.newTreeMap();
  private final Partitioner<DocumentPath> pathPartitioner;

  public PartitionedAsyncDocumentTree(
      String name,
      Map<PartitionId, AsyncDocumentTree<V>> partitions,
      Partitioner<DocumentPath> pathPartitioner) {
    this.name = name;
    this.partitions.putAll(checkNotNull(partitions));
    this.pathPartitioner = checkNotNull(pathPartitioner);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public DocumentPath root() {
    return DocumentPath.ROOT;
  }

  /**
   * Returns the document tree (partition) to which the specified path maps.
   *
   * @param path path
   * @return AsyncConsistentMap to which path maps
   */
  private AsyncDocumentTree<V> partition(DocumentPath path) {
    return partitions.get(pathPartitioner.partition(path));
  }

  /**
   * Returns all the constituent trees.
   *
   * @return collection of partitions.
   */
  private Collection<AsyncDocumentTree<V>> partitions() {
    return partitions.values();
  }

  @Override
  public CompletableFuture<Map<String, Versioned<V>>> getChildren(DocumentPath path) {
    return Futures.allOf(partitions().stream()
        .map(partition -> partition.getChildren(path).exceptionally(r -> null))
        .collect(Collectors.toList()))
        .thenApply(allChildren -> {
          Map<String, Versioned<V>> children = Maps.newLinkedHashMap();
          allChildren.stream().filter(Objects::nonNull).forEach(children::putAll);
          return children;
        });
  }

  @Override
  public CompletableFuture<Versioned<V>> get(DocumentPath path) {
    return partition(path).get(path);
  }

  @Override
  public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
    return partition(path).set(path, value);
  }

  @Override
  public CompletableFuture<Boolean> create(DocumentPath path, V value) {
    return partition(path).create(path, value);
  }

  @Override
  public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
    return partition(path).createRecursive(path, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
    return partition(path).replace(path, newValue, version);
  }

  @Override
  public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
    return partition(path).replace(path, newValue, currentValue);
  }

  @Override
  public CompletableFuture<Versioned<V>> removeNode(DocumentPath path) {
    return partition(path).removeNode(path);
  }

  @Override
  public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<V> listener) {
    return CompletableFuture.allOf(partitions().stream()
        .map(map -> map.addListener(path, listener))
        .toArray(CompletableFuture[]::new));
  }

  @Override
  public CompletableFuture<Void> removeListener(DocumentTreeListener<V> listener) {
    return CompletableFuture.allOf(partitions().stream()
        .map(map -> map.removeListener(listener))
        .toArray(CompletableFuture[]::new));
  }

  @Override
  public CompletableFuture<Void> destroy() {
    return Futures.allOf(partitions()
        .stream()
        .map(AsyncPrimitive::destroy)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.allOf(partitions()
        .stream()
        .map(AsyncPrimitive::close)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  @Override
  public DocumentTree<V> sync(Duration operationTimeout) {
    return new BlockingDocumentTree<>(this, operationTimeout.toMillis());
  }
}
