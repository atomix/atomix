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
package net.kuujo.copycat.io.serializer;

import net.kuujo.copycat.CopycatException;

/**
 * Default serializer factory.
 * <p>
 * The default serializer factory constructs {@link TypeSerializer} instances given a serializer {@link Class}. The serializer
 * must implement a default no-argument constructor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultTypeSerializerFactory implements TypeSerializerFactory {
  private final Class<? extends TypeSerializer> type;

  public DefaultTypeSerializerFactory(Class<? extends TypeSerializer> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  @Override
  public TypeSerializer<?> createSerializer(Class<?> type) {
    try {
      return this.type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new CopycatException("failed to instantiate serializer: " + this.type, e);
    }
  }

}
