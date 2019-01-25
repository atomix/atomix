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

package io.atomix.core.tree.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTreeNode;
import io.atomix.primitive.Ordering;
import io.atomix.utils.time.Versioned;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@code DocumentTree} node.
 */
public class DefaultDocumentTreeNode<V> implements DocumentTreeNode<V> {
  private final DocumentPath key;
  private Versioned<V> value;
  private final Map<String, DocumentTreeNode<V>> children;
  private final Ordering ordering;
  private final DocumentTreeNode<V> parent;

  public DefaultDocumentTreeNode(DocumentPath key,
                                 V value,
                                 long version,
                                 Ordering ordering,
                                 DocumentTreeNode<V> parent) {
    this.key = checkNotNull(key);
    this.value = new Versioned<>(value, version);
    this.ordering = ordering;
    this.parent = parent;

    switch (ordering) {
      case INSERTION:
        children = Maps.newLinkedHashMap();
        break;
      case NATURAL:
      default:
        children = Maps.newTreeMap();
        break;
    }
  }

  @Override
  public DocumentPath path() {
    return key;
  }

  @Override
  public Versioned<V> value() {
    return value;
  }

  @Override
  public Iterator<DocumentTreeNode<V>> children() {
    return ImmutableList.copyOf(children.values()).iterator();
  }

  @Override
  public DocumentTreeNode<V> child(String name) {
    return children.get(name);
  }

  public DocumentTreeNode<V> parent() {
    return parent;
  }

  /**
   * Adds a new child only if one does not exist with the name.
   *
   * @param name       relative path name of the child node
   * @param newValue   new value to set
   * @param newVersion new version to set
   * @return previous value; can be {@code null} if no child currently exists with that relative path name.
   * a non null return value indicates child already exists and no modification occurred.
   */
  public Versioned<V> addChild(String name, V newValue, long newVersion) {
    DefaultDocumentTreeNode<V> child = (DefaultDocumentTreeNode<V>) children.get(name);
    if (child != null) {
      return child.value();
    }
    children.put(name, new DefaultDocumentTreeNode<>(
        new DocumentPath(name, path()), newValue, newVersion, ordering, this));
    return null;
  }

  /**
   * Updates the node value.
   *
   * @param newValue   new value to set
   * @param newVersion new version to set
   * @return previous value
   */
  public Versioned<V> update(V newValue, long newVersion) {
    Versioned<V> previousValue = value;
    value = new Versioned<>(newValue, newVersion);
    return previousValue;
  }


  /**
   * Removes a child node.
   *
   * @param name the name of child node to be removed
   * @return {@code true} if the child set was modified as a result of this call, {@code false} otherwise
   */
  public boolean removeChild(String name) {
    return children.remove(name) != null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.key);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DefaultDocumentTreeNode) {
      DefaultDocumentTreeNode<V> that = (DefaultDocumentTreeNode<V>) obj;
      if (this.parent.equals(that.parent)) {
        if (this.children.size() == that.children.size()) {
          return Sets.symmetricDifference(this.children.keySet(), that.children.keySet()).isEmpty();
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper =
        MoreObjects.toStringHelper(getClass())
            .add("parent", this.parent)
            .add("key", this.key)
            .add("value", this.value);
    for (DocumentTreeNode<V> child : children.values()) {
      helper = helper.add("child", "\n" + child.path().pathElements()
          .get(child.path().pathElements().size() - 1) + " : " + child.value());
    }
    return helper.toString();
  }
}
