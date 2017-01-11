/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.collections.internal;

import io.atomix.catalyst.util.Assert;

import java.util.Map;

/**
 * Map entry implementation shared by various map utilities.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapEntry<K, V> implements Map.Entry<K, V> {
  private final K key;
  private V value;

  public MapEntry(K key, V value) {
    this.key = Assert.notNull(key, "key");
    this.value = value;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public V setValue(V value) {
    V oldValue = this.value;
    this.value = value;
    return oldValue;
  }
}
