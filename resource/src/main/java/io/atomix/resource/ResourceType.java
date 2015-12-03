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
package io.atomix.resource;

import io.atomix.catalyst.util.Assert;

/**
 * Resource type.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ResourceType<T extends Resource> {
  private final Class<T> type;
  private final int id;
  private final Class<? extends ResourceStateMachine> stateMachine;

  public ResourceType(Class<T> type) {
    this.type = Assert.notNull(type, "type");

    ResourceTypeInfo info = type.getAnnotation(ResourceTypeInfo.class);
    if (info == null) {
      throw new IllegalArgumentException("resource type not annotated");
    }

    this.id = info.id();
    this.stateMachine = info.stateMachine();
  }

  /**
   * Returns the resource type ID.
   *
   * @return The resource type ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the resource class.
   *
   * @return The resource class.
   */
  public Class<T> resource() {
    return type;
  }

  /**
   * Returns the resource state machine class.
   *
   * @return The resource state machine class.
   */
  public Class<? extends ResourceStateMachine> stateMachine() {
    return stateMachine;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + type.hashCode();
    hashCode = 37 * hashCode + id;
    hashCode = 37 * hashCode + stateMachine.hashCode();
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ResourceType) {
      ResourceType resourceType = (ResourceType) object;
      return resourceType.type == type && resourceType.id == id && resourceType.stateMachine == stateMachine;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, type=%s, state=%s]", getClass().getSimpleName(), id, type, stateMachine);
  }

}
