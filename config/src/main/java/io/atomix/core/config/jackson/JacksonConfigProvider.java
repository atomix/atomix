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
package io.atomix.core.config.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.atomix.core.config.ConfigProvider;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveProtocolConfig;
import io.atomix.primitive.PrimitiveProtocols;
import io.atomix.primitive.PrimitiveTypes;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroups;
import io.atomix.utils.Config;
import io.atomix.utils.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Jackson configuration provider.
 */
public class JacksonConfigProvider implements ConfigProvider {
  private static final String YAML_EXT = ".yaml";
  private static final String YML_EXT = ".yml";
  private static final String JSON_EXT = ".json";

  @Override
  public boolean isConfigFile(File file) {
    return isYaml(file) || isJson(file);
  }

  private boolean isYaml(File file) {
    return file.getName().endsWith(YAML_EXT) || file.getName().endsWith(YML_EXT);
  }

  private boolean isJson(File file) {
    return file.getName().endsWith(JSON_EXT);
  }

  @Override
  public <C extends Config> C load(File file, Class<C> type) {
    if (isYaml(file)) {
      return loadYaml(file, type);
    } else if (isJson(file)) {
      return loadJson(file, type);
    } else {
      throw new ConfigurationException("Unknown file type: " + file.getName());
    }
  }

  private <C extends Config> C loadYaml(File file, Class<C> type) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    setupObjectMapper(mapper);
    try {
      return mapper.readValue(file, type);
    } catch (IOException e) {
      throw new ConfigurationException("Failed to parse YAML file", e);
    }
  }

  private <C extends Config> C loadJson(File file, Class<C> type) {
    ObjectMapper mapper = new ObjectMapper();
    setupObjectMapper(mapper);
    try {
      return mapper.readValue(file, type);
    } catch (IOException e) {
      throw new ConfigurationException("Failed to parse JSON file", e);
    }
  }

  private void setupObjectMapper(ObjectMapper mapper) {
    mapper.setPropertyNamingStrategy(new ConfigPropertyNamingStrategy());
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);

    SimpleModule module = new SimpleModule("PolymorphicTypes");
    module.addDeserializer(PartitionGroupConfig.class, new PartitionGroupDeserializer());
    module.addDeserializer(PrimitiveProtocolConfig.class, new PrimitiveProtocolDeserializer());
    module.addDeserializer(PrimitiveConfig.class, new PrimitiveConfigDeserializer());
    mapper.registerModule(module);
  }

  private static class ConfigPropertyNamingStrategy extends PropertyNamingStrategy.KebabCaseStrategy {
    private static final String CONFIG_SUFFIX = "Config";

    @Override
    public String translate(String input) {
      if (input.endsWith(CONFIG_SUFFIX)) {
        return super.translate(input.substring(0, input.length() - CONFIG_SUFFIX.length()));
      }
      return super.translate(input);
    }
  }

  /**
   * Polymorphic type deserializer.
   */
  private abstract static class PolymorphicTypeDeserializer<T> extends StdDeserializer<T> {
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

  /**
   * Partition group deserializer.
   */
  private static class PartitionGroupDeserializer extends PolymorphicTypeDeserializer<PartitionGroupConfig> {
    @SuppressWarnings("unchecked")
    public PartitionGroupDeserializer() {
      super(PartitionGroup.class, type -> PartitionGroups.getGroupFactory(type).configClass());
    }
  }

  /**
   * Primitive protocol deserializer.
   */
  private static class PrimitiveProtocolDeserializer extends PolymorphicTypeDeserializer<PrimitiveProtocolConfig> {
    @SuppressWarnings("unchecked")
    public PrimitiveProtocolDeserializer() {
      super(PrimitiveProtocolConfig.class, type -> PrimitiveProtocols.getProtocolFactory(type).configClass());
    }
  }

  /**
   * Primitive configuration deserializer.
   */
  private static class PrimitiveConfigDeserializer extends PolymorphicTypeDeserializer<PrimitiveConfig> {
    @SuppressWarnings("unchecked")
    public PrimitiveConfigDeserializer() {
      super(PrimitiveConfig.class, type -> PrimitiveTypes.getPrimitiveType(type).configClass());
    }
  }
}
