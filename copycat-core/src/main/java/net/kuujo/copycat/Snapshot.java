/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * State machine snapshot.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonAutoDetect(
  creatorVisibility=JsonAutoDetect.Visibility.NONE,
  fieldVisibility=JsonAutoDetect.Visibility.ANY,
  getterVisibility=JsonAutoDetect.Visibility.NONE,
  isGetterVisibility=JsonAutoDetect.Visibility.NONE,
  setterVisibility=JsonAutoDetect.Visibility.NONE
)
public class Snapshot implements Serializable {
  private static final long serialVersionUID = 5136981960509123827L;
  private final Map<String, Object> data;

  public Snapshot() {
    this(new HashMap<String, Object>());
  }

  public Snapshot(Map<String, Object> data) {
    this.data = data;
  }

  /**
   * Puts a value in the snapshot.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return The snapshot.
   */
  public Snapshot put(String key, Object value) {
    data.put(key, value);
    return this;
  }

  /**
   * Gets a value from the snapshot.
   *
   * @param key The key to get.
   * @return The snapshot value.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String key) {
    return (T) data.get(key);
  }

  /**
   * Gets a value from the snapshot.
   *
   * @param key The key to get.
   * @param type The type of the value.
   * @return The snapshot value.
   */
  public <T> T get(String key, Class<T> type) {
    return type.cast(data.get(key));
  }

  /**
   * Returns a set of keys in the snapshot.
   *
   * @return A set of keys in the snapshot.
   */
  public Set<String> keys() {
    return data.keySet();
  }

  /**
   * Returns a boolean indicating whether the snapshot contains a key.
   *
   * @param key The key to check.
   * @return Indicates whether the snapshot contains the given key.
   */
  public boolean contains(String key) {
    return data.containsKey(key);
  }

  /**
   * Removes a key from the snapshot.
   *
   * @param key The key to remove.
   * @return The snapshot.
   */
  public Snapshot remove(String key) {
    data.remove(key);
    return this;
  }

  /**
   * Clears the snapshot.
   */
  public void clear() {
    data.clear();
  }

}
