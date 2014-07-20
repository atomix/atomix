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
package net.kuujo.copycat.serializer;

import net.kuujo.copycat.util.ServiceInfo;
import net.kuujo.copycat.util.ServiceLoader;

/**
 * Serializer factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class SerializerFactory {
  private static Serializer serializer;

  /**
   * Returns the current serializer instance.
   *
   * @return A singleton serializer instance.
   */
  @SuppressWarnings("unchecked")
  public static Serializer getSerializer() {
    if (serializer == null) {
      ServiceInfo info = ServiceLoader.load("net.kuujo.copycat.Serializer");
      Class<? extends SerializerFactory> factoryClass = info.getProperty("class", Class.class);
      try {
        SerializerFactory factory = factoryClass.newInstance();
        serializer = factory.createSerializer();
      }
      catch (InstantiationException | IllegalAccessException e) {
        throw new SerializationException(e.getMessage());
      }
    }
    return serializer;
  }

  /**
   * Creates a new serializer instance.
   *
   * @return A new serializer instance.
   */
  public abstract Serializer createSerializer();

}
