// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.tree;

import io.atomix.utils.time.Versioned;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;

/**
 * A {@code DocumentTree} node.
 *
 * @param <V> value type
 */
@NotThreadSafe
public interface DocumentTreeNode<V> {

  /**
   * Returns the path to this node in a {@code DocumentTree}.
   *
   * @return absolute path
   */
  DocumentPath path();

  /**
   * Returns the value of this node.
   *
   * @return node value (and version)
   */
  Versioned<V> value();

  /**
   * Returns the children of this node.
   *
   * @return iterator for this node's children
   */
  Iterator<DocumentTreeNode<V>> children();

  /**
   * Returns the child node of this node with the specified relative path name.
   *
   * @param relativePath relative path name for the child node.
   * @return child node; this method returns {@code null} if no such child exists
   */
  DocumentTreeNode<V> child(String relativePath);

  /**
   * Returns if this node has one or more children.
   *
   * @return {@code true} if yes, {@code false} otherwise
   */
  default boolean hasChildren() {
    return children().hasNext();
  }
}
