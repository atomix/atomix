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
package io.atomix.manager.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.resource.ResourceType;

import java.util.Properties;

/**
 * Get resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GetResource extends KeyOperation<Long> implements Command<Long> {
  private ResourceType type;
  private Properties config;

  public GetResource() {
  }

  /**
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  public GetResource(String key, ResourceType type, Properties config) {
    super(key);
    this.type = Assert.notNull(type, "type");
    this.config = config;
  }

  @Override
  public CompactionMode compaction() {
    return CompactionMode.QUORUM;
  }

  /**
   * Returns the resource type.
   *
   * @return The resource type.
   */
  public ResourceType type() {
    return type;
  }

  /**
   * Returns the resource configuration.
   *
   * @return The resource configuration.
   */
  public Properties config() {
    return config;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(type, buffer);
    serializer.writeObject(config, buffer);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    type = serializer.readObject(buffer);
    config = serializer.readObject(buffer);
  }

}
