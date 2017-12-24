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

import com.google.common.base.MoreObjects;
import io.atomix.utils.time.Versioned;

import java.util.Optional;

/**
 * A document tree modification event.
 *
 * @param <V> tree node value type
 */
public class DocumentTreeEvent<V> {

  /**
   * Nature of document tree node change.
   */
  public enum Type {

    /**
     * Signifies node being created.
     */
    CREATED,

    /**
     * Signifies the value of an existing node being updated.
     */
    UPDATED,

    /**
     * Signifies an existing node being deleted.
     */
    DELETED
  }

  private final DocumentPath path;
  private final Type type;
  private final Optional<Versioned<V>> newValue;
  private final Optional<Versioned<V>> oldValue;

  @SuppressWarnings("unused")
  private DocumentTreeEvent() {
    this.path = null;
    this.type = null;
    this.newValue = null;
    this.oldValue = null;
  }

  /**
   * Constructs a new {@code DocumentTreeEvent}.
   *
   * @param path     path to the node
   * @param type     type of change
   * @param newValue optional new value; will be empty if node was deleted
   * @param oldValue optional old value; will be empty if node was created
   */
  public DocumentTreeEvent(DocumentPath path,
                           Type type,
                           Optional<Versioned<V>> newValue,
                           Optional<Versioned<V>> oldValue) {
    this.path = path;
    this.type = type;
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /**
   * Returns the path to the changed node.
   *
   * @return node path
   */
  public DocumentPath path() {
    return path;
  }

  /**
   * Returns the change type.
   *
   * @return change type
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the new value.
   *
   * @return optional new value; will be empty if node was deleted
   */
  public Optional<Versioned<V>> newValue() {
    return newValue;
  }

  /**
   * Returns the old value.
   *
   * @return optional old value; will be empty if node was created
   */
  public Optional<Versioned<V>> oldValue() {
    return oldValue;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("path", path)
        .add("type", type)
        .add("newValue", newValue)
        .add("oldValue", oldValue)
        .toString();
  }
}
