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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

/**
 * Identifier for resource metadata and {@link ResourceStateMachine state machine} information.
 * <p>
 * Given a {@link Resource} class, the {@code ResourceType} provides information about that resource
 * necessary to handle state changes and serialization in the cluster. Resource classes provided to
 * the {@code ResourceType} constructor must be annotated with the {@link ResourceTypeInfo} annotation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ResourceType implements CatalystSerializable {
  private int id;
  private Class<? extends ResourceFactory<?>> factory;

  public ResourceType() {
  }

  public ResourceType(Class<? extends Resource> type) {
    ResourceTypeInfo info = type.getAnnotation(ResourceTypeInfo.class);
    if (info == null) {
      throw new IllegalArgumentException("resource type not annotated");
    }

    this.id = info.id();
    this.factory = info.factory();
  }

  public ResourceType(int id, Class<? extends ResourceFactory<?>> factory) {
    this.id = id;
    this.factory = Assert.notNull(factory, "factory");
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
   * Returns the resource instance factory.
   *
   * @return The resource instance factory.
   */
  public Class<? extends ResourceFactory<?>> factory() {
    return factory;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeInt(id);
    serializer.writeObject(factory, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    id = buffer.readInt();
    factory = serializer.<Class<? extends ResourceFactory<?>>>readObject(buffer);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id;
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ResourceType) {
      ResourceType resourceType = (ResourceType) object;
      return resourceType.id == id;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, factory=%s]", getClass().getSimpleName(), id, factory);
  }

}
