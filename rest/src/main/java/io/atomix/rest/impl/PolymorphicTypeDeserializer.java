/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.rest.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.atomix.utils.config.ConfigurationException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Polymorphic type deserializer.
 */
public abstract class PolymorphicTypeDeserializer<T> extends StdDeserializer<T> {
  private static final String TYPE_KEY = "type";

  private final Function<String, Class<? extends T>> concreteFactory;

  protected PolymorphicTypeDeserializer(Class<?> type, Function<String, Class<? extends T>> concreteFactory) {
    super(type);
    this.concreteFactory = concreteFactory;
  }

  @Override
  public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    ObjectMapper mapper = (ObjectMapper) p.getCodec();
    ObjectNode root = mapper.readTree(p);
    Iterator<Map.Entry<String, JsonNode>> iterator = root.fields();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      if (entry.getKey().equals(TYPE_KEY)) {
        Class<? extends T> configClass = concreteFactory.apply(entry.getValue().asText());
        root.remove(TYPE_KEY);
        return mapper.convertValue(root, configClass);
      }
    }
    throw new ConfigurationException("Failed to deserialize polymorphic " + _valueClass.getSimpleName() + " configuration");
  }
}
