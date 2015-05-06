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
 * limitations under the License.
 */
package net.kuujo.copycat.collections;

import net.kuujo.copycat.log.CommitLog;
import net.kuujo.copycat.log.SharedCommitLog;
import net.kuujo.copycat.resource.Command;
import net.kuujo.copycat.resource.Resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous multi map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public class AsyncMultiMap<K, V> extends Resource<AsyncMultiMap<K, V>> {
  private Map<K, Collection<V>> map = new HashMap<>();

  public AsyncMultiMap(String name, SharedCommitLog log) {
    super(name, log);
  }

  public AsyncMultiMap(CommitLog log) {
    super(log);
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key) {
    return submit("containsKey", key);
  }

  @Command(value="containsKey", type=Command.Type.READ)
  protected boolean applyContainsKey(K key) {
    return map.containsKey(key);
  }

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return submit("containsEntry", key, value);
  }

  @Command(value="containsEntry", type=Command.Type.READ)
  protected boolean applyContainsEntry(K key, V value) {
    return map.containsKey(key) && map.get(key).contains(value);
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> get(K key) {
    return submit("get", key);
  }

  @Command(value="get", type=Command.Type.READ)
  protected Collection<V> applyGet(K key) {
    return map.get(key);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> put(K key, V value) {
    return submit("put", key, value);
  }

  @Command(value="put", type=Command.Type.WRITE)
  protected Collection<V> applyPut(K key, V value) {
    Collection<V> values = map.get(key);
    if (values == null) {
      values = new ArrayList<>();
      map.put(key, values);
    }
    values.add(value);
    return values;
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> remove(K key) {
    return submit("removeKey", key);
  }

  @Command(value="removeKey", type=Command.Type.DELETE)
  protected Collection<V> applyRemove(K key) {
    return map.remove(key);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(K key, V value) {
    return submit("removeKeyValue", key, value);
  }

  @Command(value="removeKeyValue", type=Command.Type.DELETE)
  protected boolean applyRemove(K key, V value) {
    Collection<V> values = map.get(key);
    if (values != null) {
      boolean result = values.remove(value);
      if (values.isEmpty()) {
        map.remove(key);
      }
      return result;
    }
    return false;
  }

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> getOrDefault(K key, Collection<V> defaultValue) {
    return submit("getOrDefault", key, defaultValue);
  }

  @Command(value="getOrDefault", type=Command.Type.READ)
  protected Collection<V> applyGetOrDefault(K key, Collection<V> defaultValue) {
    return map.getOrDefault(key, defaultValue);
  }

  /**
   * Replaces a key and value in the map.
   *
   * @param key The key to replace.
   * @param oldValue The value to replace.
   * @param newValue The value with which to replace the given key and value.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return submit("replace", key, oldValue, newValue);
  }

  @Command(value="replace", type=Command.Type.WRITE)
  protected boolean applyReplace(K key, V oldValue, V newValue) {
    Collection<V> values = map.get(key);
    if (values != null && values.remove(oldValue)) {
      values.add(newValue);
      return true;
    }
    return false;
  }

  /**
   * Replaces a key with the given value.
   *
   * @param key The key to replace.
   * @param value The value with which to replace the given key.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> replace(K key, Collection<V> value) {
    return submit("replaceKeyValue", key, value);
  }

  @Command(value="replaceKeyValue", type=Command.Type.WRITE)
  protected Collection<V> applyReplace(K key, Collection<V> value) {
    return map.replace(key, value);
  }

}
