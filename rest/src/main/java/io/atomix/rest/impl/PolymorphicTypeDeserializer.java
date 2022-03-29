// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
