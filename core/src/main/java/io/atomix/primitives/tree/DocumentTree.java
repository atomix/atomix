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

package io.atomix.primitives.tree;

import io.atomix.time.Versioned;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;

/**
 * A hierarchical <a href="https://en.wikipedia.org/wiki/Document_Object_Model">document tree</a> data structure.
 *
 * @param <V> document tree value type
 */
@NotThreadSafe
public interface DocumentTree<V> {

  /**
   * Returns the {@link DocumentPath path} to root of the tree.
   *
   * @return path to root of the tree
   */
  DocumentPath root();

  /**
   * Returns the child values for this node.
   *
   * @param path path to the node
   * @return mapping from a child name to its value
   * @throws NoSuchDocumentPathException if the path does not point to a valid node
   */
  Map<String, Versioned<V>> getChildren(DocumentPath path);

  /**
   * Returns a document tree node.
   *
   * @param path path to node
   * @return node value or {@code null} if path does not point to a valid node
   */
  Versioned<V> get(DocumentPath path);

  /**
   * Creates or updates a document tree node.
   *
   * @param path  path for the node to create or update
   * @param value the non-null value to be associated with the key
   * @return the previous mapping or {@code null} if there was no previous mapping
   * @throws NoSuchDocumentPathException if the parent node (for the node to create/update) does not exist
   */
  Versioned<V> set(DocumentPath path, V value);

  /**
   * Creates a document tree node if one does not exist already.
   *
   * @param path  path for the node to create
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   */
  boolean create(DocumentPath path, V value);

  /**
   * Creates a document tree node by first creating any missing intermediate nodes in the path.
   *
   * @param path  path for the node to create
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} if
   * a node already exists at that path
   * @throws IllegalDocumentModificationException if {@code path} points to root
   */
  boolean createRecursive(DocumentPath path, V value);

  /**
   * Conditionally updates a tree node if the current version matches a specified version.
   *
   * @param path     path for the node to create
   * @param newValue the non-null value to be associated with the key
   * @param version  current version of the value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   */
  boolean replace(DocumentPath path, V newValue, long version);

  /**
   * Conditionally updates a tree node if the current value matches a specified value.
   *
   * @param path         path for the node to create
   * @param newValue     the non-null value to be associated with the key
   * @param currentValue current value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise.
   * This method returns {@code false} if the newValue and currentValue are same.
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   */
  boolean replace(DocumentPath path, V newValue, V currentValue);

  /**
   * Removes the node with the specified path.
   *
   * @param path path for the node to remove
   * @return the previous value of the node or {@code null} if it did not exist
   * @throws IllegalDocumentModificationException if the remove to be removed
   */
  Versioned<V> removeNode(DocumentPath path);

  /**
   * Registers a listener to be notified when a subtree rooted at the specified path
   * is modified.
   *
   * @param path     path to root of subtree to monitor for updates
   * @param listener listener to be notified
   */
  void addListener(DocumentPath path, DocumentTreeListener<V> listener);

  /**
   * Unregisters a previously added listener.
   *
   * @param listener listener to unregister
   */
  void removeListener(DocumentTreeListener<V> listener);

  /**
   * Registers a listener to be notified when the tree is modified.
   *
   * @param listener listener to be notified
   */
  default void addListener(DocumentTreeListener<V> listener) {
    addListener(root(), listener);
  }
}
