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

import java.util.List;
import java.util.Map;

import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.json.impl.Json;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json serializer.
 * <p>
 * 
 * Via serializers serialize objects to Vert.x {@link JsonObject} instances for
 * easy passage over the Vert.x event bus.
 * 
 * @author Jordan Halterman
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
   * Serializes a value to a string.
   *
   * @param object The object to serialize.
   * @return The serialized object.
   */
  public String writeString(Object object) {
    try {
      return mapper.writeValueAsString(object);
    }
    catch (Exception e) {
      throw new SerializationException(e.getMessage());
    }
  }

  /**
   * Deserializes an object from a string.
   *
   * @param json The serialized object.
   * @param type The serialized type.
   * @return The deserialized object.
   */
  public <T> T readString(String json, Class<T> type) {
    try {
      return (T) mapper.readValue(json, type);
    }
    catch (Exception e) {
      throw new SerializationException(e.getMessage());
    }
  }

  /**
   * Serializes an object to Json. If an error occurs during serialization, a
   * {@link SerializationException} will be thrown.
   * 
   * @param object The object to serialize.
   * @return A Json representation of the serializable object.
   * @throws SerializationException If an error occurs during serialization.
   */
  @SuppressWarnings("unchecked")
  public <T extends JsonElement> T writeObject(Object object) {
    String value;
    try {
      value = mapper.writeValueAsString(object);
    }
    catch (Exception e) {
      throw new SerializationException(e.getMessage());
    }

    try {
      Map<String, Object> map = Json.decodeValue(value, Map.class);
      return (T) new JsonObject(map);
    }
    catch (DecodeException e) {
      List<Object> list = Json.decodeValue(value, List.class);
      return (T) new JsonArray(list);
    }
  }

  /**
   * Deserializes an object from Json. If an error occurs during
   * deserialization, a {@link DeserializationException} will be thrown.
   * 
   * @param json A Json representation of the serializable object.
   * @param type The type to which to deserialize the object.
   * @return The deserialized object.
   * @throws DeserializationException If an error occurs during deserialization.
   */
  public <T> T readObject(JsonElement json, Class<T> type) {
    try {
      if (json.isArray()) {
        return mapper.readValue(json.asArray().encode(), type);
      }
      else {
        return mapper.readValue(json.asObject().encode(), type);
      }
    }
    catch (Exception e) {
      throw new SerializationException(e.getMessage());
    }
  }

}
