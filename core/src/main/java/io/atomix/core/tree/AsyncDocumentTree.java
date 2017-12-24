/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.tree;

import io.atomix.core.PrimitiveTypes;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveType;
import io.atomix.utils.time.Versioned;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A hierarchical <a href="https://en.wikipedia.org/wiki/Document_Object_Model">document tree</a> data structure.
 *
 * @param <V> document tree value type
 */
@NotThreadSafe
public interface AsyncDocumentTree<V> extends AsyncPrimitive {

  @Override
  default PrimitiveType primitiveType() {
    return PrimitiveTypes.tree();
  }

  /**
   * Returns the {@link DocumentPath path} to root of the tree.
   *
   * @return path to root of the tree
   */
  DocumentPath root();

  /**
   * Returns the children of node at specified path in the form of a mapping from child name to child value.
   *
   * @param path path to the node
   * @return future for mapping from child name to child value
   * @throws NoSuchDocumentPathException if the path does not point to a valid node
   */
  CompletableFuture<Map<String, Versioned<V>>> getChildren(DocumentPath path);

  /**
   * Returns the value of the tree node at specified path.
   *
   * @param path path to the node
   * @return future that will be either be completed with node's {@link Versioned versioned} value
   * or {@code null} if path does not point to a valid node
   */
  CompletableFuture<Versioned<V>> get(DocumentPath path);

  /**
   * Creates or updates a document tree node.
   *
   * @param path  path to the node
   * @param value value to be associated with the node ({@code null} is a valid value)
   * @return future that will either be completed with the previous {@link Versioned versioned}
   * value or {@code null} if there was no node previously at that path.
   * Future will be completed with a {@code IllegalDocumentModificationException}
   * if the parent node (for the node to create/update) does not exist
   */
  CompletableFuture<Versioned<V>> set(DocumentPath path, V value);

  /**
   * Creates a document tree node if one does not exist already.
   *
   * @param path  path to the node
   * @param value the non-null value to be associated with the node
   * @return future that is completed with {@code true} if the new node was successfully
   * created. Future will be completed with {@code false} if a node already exists at the specified path.
   * Future will be completed exceptionally with a {@code IllegalDocumentModificationException} if the parent
   * node (for the node to create) does not exist
   */
  CompletableFuture<Boolean> create(DocumentPath path, V value);

  /**
   * Creates a document tree node recursively by creating all missing intermediate nodes in the path.
   *
   * @param path  path to the node
   * @param value value to be associated with the node ({@code null} is a valid value)
   * @return future that is completed with {@code true} if the new node was successfully
   * created. Future will be completed with {@code false} if a node already exists at the specified path
   */
  CompletableFuture<Boolean> createRecursive(DocumentPath path, V value);

  /**
   * Conditionally updates a tree node if the current version matches a specified version.
   *
   * @param path     path to the node
   * @param newValue value to associate with the node ({@code null} is a valid value)
   * @param version  current version of the node for update to occur
   * @return future that is either completed with {@code true} if the update was made
   * or {@code false} if update did not happen
   */
  CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version);

  /**
   * Conditionally updates a tree node if the current node value matches a specified version.
   *
   * @param path         path to the node
   * @param newValue     value to associate with the node ({@code null} is a valid value)
   * @param currentValue current value of the node for update to occur
   * @return future that is either completed with {@code true} if the update was made
   * or {@code false} if update did not happen
   */
  CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue);

  /**
   * Removes the node with the specified path.
   *
   * @param path path to the node
   * @return future for the previous value. Future will be completed with a
   * {@code IllegalDocumentModificationException} if the node to be removed is either the root
   * node or has one or more children. Future will be completed with a
   * {@code NoSuchDocumentPathException} if the node to be removed does not exist
   */
  CompletableFuture<Versioned<V>> removeNode(DocumentPath path);

  /**
   * Registers a listener to be notified when the subtree rooted at the specified path
   * is modified.
   *
   * @param path     path to the node
   * @param listener listener to be notified
   * @return a future that is completed when the operation completes
   */
  CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<V> listener);

  /**
   * Unregisters a previously added listener.
   *
   * @param listener listener to unregister
   * @return a future that is completed when the operation completes
   */
  CompletableFuture<Void> removeListener(DocumentTreeListener<V> listener);

  /**
   * Registers a listener to be notified when the tree is modified.
   *
   * @param listener listener to be notified
   * @return a future that is completed when the operation completes
   */
  default CompletableFuture<Void> addListener(DocumentTreeListener<V> listener) {
    return addListener(root(), listener);
  }

  @Override
  default DocumentTree<V> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DocumentTree<V> sync(Duration operationTimeout);
}
