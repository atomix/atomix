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
package io.atomix.resource.util;

import io.atomix.catalyst.util.Assert;
import io.atomix.resource.ResourceType;

import java.util.function.Consumer;

/**
 * Represents an instance of a client-side resource.
 * <p>
 * A resource instance is associated with a specific {@link #key()} and {@link #type() resource type}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ResourceInstance implements AutoCloseable {
  private final String key;
  private final ResourceType type;
  private final Consumer<ResourceInstance> closer;

  public ResourceInstance(String key, ResourceType type, Consumer<ResourceInstance> closer) {
    this.key = Assert.notNull(key, "key");
    this.type = Assert.notNull(type, "type");
    this.closer = Assert.notNull(closer, "closer");
  }

  /**
   * Returns the resource instance key.
   *
   * @return The resource instance key.
   */
  public String key() {
    return key;
  }

  /**
   * Returns the resource instance type.
   *
   * @return The resource instance type.
   */
  public ResourceType type() {
    return type;
  }

  @Override
  public void close() {
    closer.accept(this);
  }

  @Override
  public String toString() {
    return String.format("%s[key=%s, type=%s]", getClass().getSimpleName(), key, type);
  }

}
