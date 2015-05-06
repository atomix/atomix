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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public class AsyncMap<K, V> extends Resource<AsyncMap<K, V>> {
  private final Map<K, V> map = new HashMap<>();

  public AsyncMap(String name, SharedCommitLog log) {
    super(name, log);
  }

  public AsyncMap(CommitLog log) {
    super(log);
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(Object key) {
    return submit("containsKey", key);
  }

  @Command(value="containsKey", type=Command.Type.READ)
  protected boolean applyContainsKey(Object key) {
    return map.containsKey(key);
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<V> get(Object key) {
    return submit("get", key);
  }

  @Command(value="get", type=Command.Type.READ)
  protected V applyGet(K key) {
    return map.get(key);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<V> put(K key, V value) {
    return submit("put", key, value);
  }

  @Command("put")
  protected V applyPut(K key, V value) {
    return map.put(key, value);
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<V> remove(Object key) {
    return submit("remove", key);
  }

  @Command(value="remove", type=Command.Type.DELETE)
  protected V applyRemove(K key) {
    return map.remove(key);
  }

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue) {
    return submit("getOrDefault", key, defaultValue);
  }

  @Command(value="getOrDefault", type=Command.Type.READ)
  protected V applyGetOrDefault(Object key, V value) {
    return map.getOrDefault(key, value);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key The key to set.
   * @param value The value to set if the given key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return submit("putIfAbsent", key, value);
  }

  @Command("putIfAbsent")
  protected V applyPutIfAbsent(K key, V value) {
    return map.putIfAbsent(key, value);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return submit("removeKeyValue", key, value);
  }

  @Command(value="removeKeyValue", type=Command.Type.DELETE)
  protected boolean applyRemove(K key, V value) {
    return map.remove(key, value);
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

  @Command("replace")
  protected boolean applyReplace(K key, V oldValue, V newValue) {
    return map.replace(key, oldValue, newValue);
  }

  /**
   * Replaces a key with the given value.
   *
   * @param key The key to replace.
   * @param value The value with which to replace the given key.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<V> replace(K key, V value) {
    return submit("replaceKeyValue", key, value);
  }

  @Command("replaceKeyValue")
  protected V applyReplace(K key, V value) {
    return map.replace(key, value);
  }

}
