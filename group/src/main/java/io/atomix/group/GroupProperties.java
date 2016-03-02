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
package io.atomix.group;

import io.atomix.catalyst.util.Assert;
import io.atomix.group.state.GroupCommands;

import java.util.concurrent.CompletableFuture;

/**
 * Group properties.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupProperties {
  private final String memberId;
  private final DistributedGroup group;

  protected GroupProperties(String memberId, DistributedGroup group) {
    this.memberId = memberId;
    this.group = Assert.notNull(group, "group");
  }

  public CompletableFuture<Void> set(String property, Object value) {
    return group.submit(new GroupCommands.SetProperty(memberId, property, value));
  }

  public <T> CompletableFuture<T> get(String property) {
    return get(property, null);
  }

  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> get(String property, T defaultValue) {
    return group.submit(new GroupCommands.GetProperty(memberId, property)).thenApply(result -> {
      if (result == null) {
        result = defaultValue;
      }
      return (T) result;
    });
  }

  public CompletableFuture<Void> remove(String property) {
    return group.submit(new GroupCommands.RemoveProperty(memberId, property));
  }

  @Override
  public String toString() {
    if (memberId == null) {
      return getClass().getSimpleName();
    }
    return String.format("%s[member=%s]", getClass().getSimpleName(), memberId);
  }

}
