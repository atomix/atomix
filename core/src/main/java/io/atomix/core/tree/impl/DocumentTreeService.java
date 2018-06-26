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
package io.atomix.core.tree.impl;

import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;
import io.atomix.utils.time.Versioned;

import java.util.Map;

/**
 * Document tree service.
 */
public interface DocumentTreeService {

  /**
   * Returns the child values for this node.
   *
   * @param path path to the node
   * @return mapping from a child name to its value
   * @throws NoSuchDocumentPathException if the path does not point to a valid node
   */
  @Query
  DocumentTreeResult<Map<String, Versioned<byte[]>>> getChildren(DocumentPath path);

  /**
   * Returns a document tree node.
   *
   * @param path path to node
   * @return node value or {@code null} if path does not point to a valid node
   */
  @Query
  Versioned<byte[]> get(DocumentPath path);

  /**
   * Creates or updates a document tree node.
   *
   * @param path  path for the node to create or update
   * @param value the non-null value to be associated with the key
   * @return the previous mapping or {@code null} if there was no previous mapping
   * @throws NoSuchDocumentPathException if the parent node (for the node to create/update) does not exist
   */
  @Command
  DocumentTreeResult<Versioned<byte[]>> set(DocumentPath path, byte[] value);

  /**
   * Creates a document tree node if one does not exist already.
   *
   * @param path  path for the node to create
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   */
  @Command
  DocumentTreeResult<Void> create(DocumentPath path, byte[] value);

  /**
   * Creates a document tree node by first creating any missing intermediate nodes in the path.
   *
   * @param path  path for the node to create
   * @param value the non-null value to be associated with the key
   * @return returns {@code true} if the mapping could be added successfully, {@code false} if
   * a node already exists at that path
   * @throws IllegalDocumentModificationException if {@code path} points to root
   */
  @Command
  DocumentTreeResult<Void> createRecursive(DocumentPath path, byte[] value);

  /**
   * Conditionally updates a tree node if the current version matches a specified version.
   *
   * @param path     path for the node to create
   * @param newValue the non-null value to be associated with the key
   * @param version  current version of the value for update to occur
   * @return returns {@code true} if the update was made and the tree was modified, {@code false} otherwise
   * @throws NoSuchDocumentPathException if the parent node (for the node to create) does not exist
   */
  @Command("replaceVersion")
  DocumentTreeResult<Void> replace(DocumentPath path, byte[] newValue, long version);

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
  @Command("replaceValue")
  DocumentTreeResult<Void> replace(DocumentPath path, byte[] newValue, byte[] currentValue);

  /**
   * Removes the node with the specified path.
   *
   * @param path path for the node to remove
   * @return the previous value of the node or {@code null} if it did not exist
   * @throws IllegalDocumentModificationException if the remove to be removed
   */
  @Command
  DocumentTreeResult<Versioned<byte[]>> removeNode(DocumentPath path);

  /**
   * Registers a listener to be notified when a subtree rooted at the specified path
   * is modified.
   *
   * @param path path to root of subtree to monitor for updates
   */
  @Command
  void listen(DocumentPath path);

  /**
   * Unregisters a previously added listener.
   *
   * @param path path to remove
   */
  @Command
  void unlisten(DocumentPath path);

  /**
   * Clears the document tree.
   */
  @Command
  void clear();

}
