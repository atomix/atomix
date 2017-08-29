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
package io.atomix.primitives;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Interface for all distributed primitives.
 */
public interface DistributedPrimitive {

  /**
   * Type of distributed primitive.
   */
  enum Type {
    /**
     * Map with strong consistency semantics.
     */
    CONSISTENT_MAP,

    /**
     * Map with eventual consistency semantics.
     */
    EVENTUALLY_CONSISTENT_MAP,

    /**
     * Consistent Multimap.
     */
    CONSISTENT_MULTIMAP,

    /**
     * Distributed set.
     */
    SET,

    /**
     * Tree map.
     */
    CONSISTENT_TREEMAP,

    /**
     * Atomic counter.
     */
    COUNTER,

    /**
     * Numeric ID generator.
     */
    ID_GENERATOR,

    /**
     * Atomic counter map.
     */
    COUNTER_MAP,

    /**
     * Atomic value.
     */
    VALUE,

    /**
     * Distributed work queue.
     */
    WORK_QUEUE,

    /**
     * Document tree.
     */
    DOCUMENT_TREE,

    /**
     * Distributed topic.
     */
    TOPIC,

    /**
     * Leader elector.
     */
    LEADER_ELECTOR,

    /**
     * Lock.
     */
    LOCK,

    /**
     * Transaction Context.
     */
    TRANSACTION_CONTEXT
  }

  /**
   * Status of distributed primitive.
   */
  enum Status {

    /**
     * Signifies a state wherein the primitive is operating correctly and is capable of meeting the advertised
     * consistency and reliability guarantees.
     */
    ACTIVE,

    /**
     * Signifies a state wherein the primitive is temporarily incapable of providing the advertised
     * consistency properties.
     */
    SUSPENDED,

    /**
     * Signifies a state wherein the primitive has been shutdown and therefore cannot perform its functions.
     */
    INACTIVE
  }

  /**
   * Default timeout for primitive operations.
   */
  long DEFAULT_OPERATION_TIMEOUT_MILLIS = 5000L;

  /**
   * Returns the name of this primitive.
   *
   * @return name
   */
  String name();

  /**
   * Returns the type of primitive.
   *
   * @return primitive type
   */
  Type primitiveType();

  /**
   * Purges state associated with this primitive.
   * <p>
   * Implementations can override and provide appropriate clean up logic for purging
   * any state state associated with the primitive. Whether modifications made within the
   * destroy method have local or global visibility is left unspecified.
   *
   * @return {@code CompletableFuture} that is completed when the operation completes
   */
  default CompletableFuture<Void> destroy() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Registers a listener to be called when the primitive's status changes.
   *
   * @param listener The listener to be called when the status changes.
   */
  default void addStatusChangeListener(Consumer<Status> listener) {
  }

  /**
   * Unregisters a previously registered listener to be called when the primitive's status changes.
   *
   * @param listener The listener to unregister
   */
  default void removeStatusChangeListener(Consumer<Status> listener) {
  }

  /**
   * Returns the collection of status change listeners previously registered.
   *
   * @return collection of status change listeners
   */
  default Collection<Consumer<Status>> statusChangeListeners() {
    return Collections.emptyList();
  }
}
