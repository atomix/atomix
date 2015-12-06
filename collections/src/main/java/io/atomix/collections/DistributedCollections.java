/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.collections;

import io.atomix.resource.ResourceType;

/**
 * Distributed collections resource types.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class DistributedCollections {

  private DistributedCollections() {
  }

  /**
   * Distributed map resource.
   */
  public static final ResourceType<DistributedMap> MAP = DistributedMap.TYPE;

  /**
   * Distributed multimap resource.
   */
  public static final ResourceType<DistributedMultiMap> MULTI_MAP = DistributedMultiMap.TYPE;

  /**
   * Distributed set resource.
   */
  public static final ResourceType<DistributedSet> SET = DistributedSet.TYPE;

  /**
   * Distributed queue resource.
   */
  public static final ResourceType<DistributedQueue> QUEUE = DistributedQueue.TYPE;

}
