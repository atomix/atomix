/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive;

/**
 * Type of distributed primitive.
 */
public enum PrimitiveTypes implements PrimitiveType {
  /**
   * Map with strong consistency semantics.
   */
  CONSISTENT_MAP,

  /**
   * Consistent Multimap.
   */
  CONSISTENT_MULTIMAP,

  /**
   * Tree map.
   */
  CONSISTENT_TREEMAP,

  /**
   * Distributed set.
   */
  SET,

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
  TRANSACTION_CONTEXT;

  @Override
  public String id() {
    return name();
  }
}