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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Command arguments.
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
public class Arguments implements Map<String, Object>, Serializable {
  private static final long serialVersionUID = 1916328200360822351L;
  private final Map<String, Object> args;

  public Arguments() {
    this(new HashMap<String, Object>());
  }

  public Arguments(Map<String, Object> args) {
    this.args = args;
  }

  @Override
  public int size() {
    return args.size();
  }

  @Override
  public boolean isEmpty() {
    return args.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return args.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return args.containsValue(value);
  }

  @Override
  public Object get(Object key) {
    return args.get(key);
  }

  /**
   * Returns a types argument value.
   *
   * @param key The argument key.
   * @param type The argument type.
   * @return The argument value.
   */
  public <T> T get(String key, Class<T> type) {
    return type.cast(args.get(key));
  }

  @Override
  public Object put(String key, Object value) {
    return args.put(key, value);
  }

  @Override
  public Object remove(Object key) {
    return args.remove(key);
  }

  @Override
  public void putAll(Map<? extends String, ? extends Object> m) {
    args.putAll(m);
  }

  @Override
  public void clear() {
    args.clear();
  }

  @Override
  public Set<String> keySet() {
    return args.keySet();
  }

  @Override
  public Collection<Object> values() {
    return args.values();
  }

  @Override
  public Set<java.util.Map.Entry<String, Object>> entrySet() {
    return args.entrySet();
  }

  @Override
  public String toString() {
    return args.toString();
  }

  @Override
  public boolean equals(Object object) {
    return args.equals(object);
  }

  @Override
  public int hashCode() {
    return args.hashCode();
  }

}
