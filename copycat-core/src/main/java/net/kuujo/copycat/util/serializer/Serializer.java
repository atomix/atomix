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
package net.kuujo.copycat.util.serializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json serializer.
 * <p>
 * 
 * Via serializers serialize objects to Vert.x {@link JsonObject} instances for
 * easy passage over the Vert.x event bus.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Serializer {
  private static Serializer instance;

  /**
   * Gets a singleton serializer instance.
   * 
   * @return A singleton serializer instance.
   */
  public static Serializer getInstance() {
    if (instance == null) {
      instance = new Serializer();
    }
    return instance;
  }

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Serializes the value to a byte array.
   *
   * @param object The object to serialize.
   * @return The serialized object.
   */
  public byte[] writeValue(Object object) {
    try {
      return mapper.writeValueAsBytes(object);
    } catch (JsonProcessingException e) {
      throw new SerializationException(e.getMessage());
    }
  }

  /**
   * Deserializes the value from a byte array.
   *
   * @param bytes The byte array to deserialize.
   * @param type The type to which to deserialize the bytes.
   * @return The deserialized object.
   */
  public <T> T readValue(byte[] bytes, Class<T> type) {
    try {
      return mapper.readValue(bytes, type);
    } catch (IOException e) {
      throw new SerializationException(e.getMessage());
    }
  }

}
