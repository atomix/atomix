// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree;

import com.google.common.base.MoreObjects;
import io.atomix.utils.event.AbstractEvent;
import io.atomix.utils.time.Versioned;

import java.util.Optional;

/**
 * A document tree modification event.
 *
 * @param <V> tree node value type
 */
public class DocumentTreeEvent<V> extends AbstractEvent<DocumentTreeEvent.Type, DocumentPath> {

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

  private final Optional<Versioned<V>> newValue;
  private final Optional<Versioned<V>> oldValue;

  @SuppressWarnings("unused")
  private DocumentTreeEvent() {
    super(null, null);
    this.newValue = null;
    this.oldValue = null;
  }

  /**
   * Constructs a new {@code DocumentTreeEvent}.
   *
   * @param type type of change
   * @param path path to the node
   * @param newValue optional new value; will be empty if node was deleted
   * @param oldValue optional old value; will be empty if node was created
   */
  public DocumentTreeEvent(Type type, DocumentPath path, Optional<Versioned<V>> newValue, Optional<Versioned<V>> oldValue) {
    super(type, path);
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /**
   * Returns the path to the changed node.
   *
   * @return node path
   */
  public DocumentPath path() {
    return subject();
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
        .add("type", type())
        .add("path", path())
        .add("newValue", newValue)
        .add("oldValue", oldValue)
        .toString();
  }
}
