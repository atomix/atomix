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
package io.atomix.core.collection.impl;

import com.google.common.collect.Sets;
import io.atomix.core.collection.DistributedSetType;

import java.util.Set;

/**
 * Default distributed set service.
 */
public class DefaultDistributedSetService extends DefaultDistributedCollectionService<Set<String>> implements DistributedSetService {
  public DefaultDistributedSetService() {
    super(DistributedSetType.instance(), Sets.newConcurrentHashSet());
  }
}
