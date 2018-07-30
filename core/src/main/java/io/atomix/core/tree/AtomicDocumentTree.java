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

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * A hierarchical <a href="https://en.wikipedia.org/wiki/Document_Object_Model">document tree</a> data structure.
 *
 * @param <V> document tree value type
 */
public interface AtomicDocumentTree<V> extends SyncPrimitive {

  /**
   * Returns the {@link DocumentPath path} to root of the tree.
   *
   * @return path to root of the tree
   */
  DocumentPath root();

  /**
   * Returns the child values for this node.
   *
   * @param path path to the node, must be prefixed with the root {@code /} path
   * @return mapping from a child name to its value
   * @throws NoSuchDocumentPathException if the path does not point to a valid node
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default Map<String, Versioned<V>> getChildren(String path) {
    return getChildren(DocumentPath.from(path));
  }

  /**
   * Returns the child values for this node.
   *
   * @param path path to the node
   * @return mapping from a child name to its value
   * @throws NoSuchDocumentPathException if the path does not point to a valid node
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  Map<String, Versioned<V>> getChildren(DocumentPath path);

  /**
   * Returns a document tree node.
   *
   * @param path path to the node, must be prefixed with the root {@code /} path
   * @return node value or {@code null} if path does not point to a valid node
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default Versioned<V> get(String path) {
    return get(DocumentPath.from(path));
  }

  /**
   * Returns a document tree node.
   *
   * @param path path to node
   * @return node value or {@code null} if path does not point to a valid node
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  Versioned<V> get(DocumentPath path);

  /**
   * Creates or updates a document tree node.
   *
   * @param path path to the node to create or update, must be prefixed with the root {@code /} path
   * @param value the non-null value to be associated with the key
   * @return the previous mapping or {@code null} if there was no previous mapping
   * @throws NoSuchDocumentPathException if the parent node (for the node to create/update) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default Versioned<V> set(String path, V value) {
    return set(DocumentPath.from(path), value);
  }

  /**
   * Creates or updates a document tree node.
   *
   * @param path path for the node to create or update
   * @param value the non-null value to be associated with the key
   * @return the previous mapping or {@code null} if there was no previous mapping
   * @throws NoSuchDocumentPathException if the parent node (for the node to create/update) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  Versioned<V> set(DocumentPath path, V value);

  /**
   * Creates a document tree node if one does not exist already.
   *
   * @param path path to the node to create, must be prefixed with the root {@code /} path
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default boolean create(String path, V value) {
    return create(DocumentPath.from(path), value);
  }

  /**
   * Creates a document tree node if one does not exist already.
   *
   * @param path path for the node to create
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  boolean create(DocumentPath path, V value);

  /**
   * Creates a document tree node by first creating any missing intermediate nodes in the path.
   *
   * @param path path to the node to create, must be prefixed with the root {@code /} path
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} if a node already exists at
   *     that path
   * @throws IllegalDocumentModificationException if {@code path} points to root
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default boolean createRecursive(String path, V value) {
    return createRecursive(DocumentPath.from(path), value);
  }

  /**
   * Creates a document tree node by first creating any missing intermediate nodes in the path.
   *
   * @param path path for the node to create
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} if a node already exists at
   *     that path
   * @throws IllegalDocumentModificationException if {@code path} points to root
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  boolean createRecursive(DocumentPath path, V value);

  /**
   * Conditionally updates a tree node if the current version matches a specified version.
   *
   * @param path path to the node to replace, must be prefixed with the root {@code /} path
   * @param newValue the non-null value to be associated with the key
   * @param version current version of the value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default boolean replace(String path, V newValue, long version) {
    return replace(DocumentPath.from(path), newValue, version);
  }

  /**
   * Conditionally updates a tree node if the current version matches a specified version.
   *
   * @param path path for the node to replace
   * @param newValue the non-null value to be associated with the key
   * @param version current version of the value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  boolean replace(DocumentPath path, V newValue, long version);

  /**
   * Conditionally updates a tree node if the current value matches a specified value.
   *
   * @param path path to the node to replace, must be prefixed with the root {@code /} path
   * @param newValue the non-null value to be associated with the key
   * @param currentValue current value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise. This method
   *     returns {@code false} if the newValue and currentValue are same.
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default boolean replace(String path, V newValue, V currentValue) {
    return replace(DocumentPath.from(path), newValue, currentValue);
  }

  /**
   * Conditionally updates a tree node if the current value matches a specified value.
   *
   * @param path path for the node to create
   * @param newValue the non-null value to be associated with the key
   * @param currentValue current value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise. This method
   *     returns {@code false} if the newValue and currentValue are same.
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  boolean replace(DocumentPath path, V newValue, V currentValue);

  /**
   * Removes the node with the specified path.
   *
   * @param path path to the node to remove, must be prefixed with the root {@code /} path
   * @return the previous value of the node or {@code null} if it did not exist
   * @throws IllegalDocumentModificationException if the remove to be removed
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  default Versioned<V> remove(String path) {
    return remove(DocumentPath.from(path));
  }

  /**
   * Removes the node with the specified path.
   *
   * @param path path for the node to remove
   * @return the previous value of the node or {@code null} if it did not exist
   * @throws IllegalDocumentModificationException if the remove to be removed
   * @throws IllegalArgumentException if the given path does not start at root {@code /}
   */
  Versioned<V> remove(DocumentPath path);

  /**
   * Registers a listener to be notified when the tree is modified.
   *
   * @param listener listener to be notified
   */
  default void addListener(DocumentTreeEventListener<V> listener) {
    addListener(root(), listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers a listener to be notified when a subtree rooted at the specified path is modified.
   *
   * @param path path to root of subtree to monitor for updates
   * @param listener listener to be notified
   */
  default void addListener(DocumentPath path, DocumentTreeEventListener<V> listener) {
    addListener(path, listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers a listener to be notified when the tree is modified.
   *
   * @param listener listener to be notified
   * @param executor the executor on which to notify the event listener
   */
  default void addListener(DocumentTreeEventListener<V> listener, Executor executor) {
    addListener(null, listener, executor);
  }

  /**
   * Registers a listener to be notified when a subtree rooted at the specified path is modified.
   *
   * @param path path to root of subtree to monitor for updates
   * @param listener listener to be notified
   * @param executor the executor on which to notify the event listener
   */
  void addListener(DocumentPath path, DocumentTreeEventListener<V> listener, Executor executor);

  /**
   * Unregisters a previously added listener.
   *
   * @param listener listener to unregister
   */
  void removeListener(DocumentTreeEventListener<V> listener);

  @Override
  AsyncAtomicDocumentTree<V> async();
}
