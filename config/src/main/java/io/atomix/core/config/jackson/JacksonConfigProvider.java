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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import io.atomix.cluster.profile.ClusterProfile;
import io.atomix.cluster.profile.NodeProfile;
import io.atomix.core.config.jackson.impl.ClusterProfileDeserializer;
import io.atomix.core.config.jackson.impl.ConfigPropertyNamingStrategy;
import io.atomix.core.config.jackson.impl.MemberFilterDeserializer;
import io.atomix.core.config.jackson.impl.NodeProfileDeserializer;
import io.atomix.core.config.jackson.impl.PartitionGroupDeserializer;
import io.atomix.core.config.jackson.impl.PrimitiveConfigDeserializer;
import io.atomix.core.config.jackson.impl.PrimitiveProtocolDeserializer;
import io.atomix.core.config.jackson.impl.ProfileDeserializer;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.partition.MemberFilter;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.utils.config.Config;
import io.atomix.utils.config.ConfigProvider;
import io.atomix.utils.config.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  private boolean isJson(String config) {
    return config.trim().startsWith("{") && config.trim().endsWith("}");
  }

  @Override
  public <C extends Config> C load(String config, Class<C> type) {
    if (isJson(config)) {
      return loadJson(config, type);
    } else {
      return loadYaml(config, type);
    }
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
    ObjectMapper mapper = new ObjectMapper(new InterpolatingYamlFactory());
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

  private <C extends Config> C loadYaml(String config, Class<C> type) {
    ObjectMapper mapper = new ObjectMapper(new InterpolatingYamlFactory());
    setupObjectMapper(mapper);
    try {
      return mapper.readValue(config, type);
    } catch (IOException e) {
      throw new ConfigurationException("Failed to parse YAML file", e);
    }
  }

  private <C extends Config> C loadJson(String config, Class<C> type) {
    ObjectMapper mapper = new ObjectMapper();
    setupObjectMapper(mapper);
    try {
      return mapper.readValue(config, type);
    } catch (IOException e) {
      throw new ConfigurationException("Failed to parse JSON file", e);
    }
  }

  private void setupObjectMapper(ObjectMapper mapper) {
    mapper.setPropertyNamingStrategy(new ConfigPropertyNamingStrategy());
    mapper.setVisibility(mapper.getVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.ANY)
        .withSetterVisibility(JsonAutoDetect.Visibility.ANY));
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);

    SimpleModule module = new SimpleModule("PolymorphicTypes");
    module.addDeserializer(PartitionGroupConfig.class, new PartitionGroupDeserializer());
    module.addDeserializer(MemberFilter.class, new MemberFilterDeserializer());
    module.addDeserializer(PrimitiveProtocolConfig.class, new PrimitiveProtocolDeserializer());
    module.addDeserializer(PrimitiveConfig.class, new PrimitiveConfigDeserializer());
    module.addDeserializer(Profile.class, new ProfileDeserializer());
    module.addDeserializer(ClusterProfile.class, new ClusterProfileDeserializer());
    module.addDeserializer(NodeProfile.class, new NodeProfileDeserializer());
    mapper.registerModule(module);
  }

  private static class InterpolatingYamlFactory extends YAMLFactory {
    @Override
    protected YAMLParser _createParser(InputStream in, IOContext ctxt) throws IOException {
      return new InterpolatingYamlParser(ctxt, _getBufferRecycler(), _parserFeatures, _yamlParserFeatures, _objectCodec, _createReader(in, null, ctxt));
    }

    @Override
    protected YAMLParser _createParser(Reader r, IOContext ctxt) throws IOException {
      return new InterpolatingYamlParser(ctxt, _getBufferRecycler(), _parserFeatures, _yamlParserFeatures, _objectCodec, r);
    }
  }

  private static class InterpolatingYamlParser extends YAMLParser {
    private final Pattern sysPattern = Pattern.compile("\\$\\{sys:([A-Za-z0-9-_.]+)\\}");
    private final Pattern envPattern = Pattern.compile("\\$\\{env:([A-Za-z0-9_]+)\\}");

    InterpolatingYamlParser(IOContext ctxt, BufferRecycler br, int parserFeatures, int formatFeatures, ObjectCodec codec, Reader reader) {
      super(ctxt, br, parserFeatures, formatFeatures, codec, reader);
    }

    @Override
    public String getText() throws IOException {
      String value = super.getText();
      return value != null ? interpolateString(value) : null;
    }

    @Override
    public String getValueAsString() throws IOException {
      return getValueAsString(null);
    }

    @Override
    public String getValueAsString(String defaultValue) throws IOException {
      String value = super.getValueAsString(defaultValue);
      return value != null ? interpolateString(value) : null;
    }

    private String interpolateString(String value) {
      value = interpolate(value, sysPattern, name -> System.getProperty(name));
      value = interpolate(value, envPattern, name -> System.getenv(name));
      return value;
    }

    private String interpolate(String value, Pattern pattern, Function<String, String> supplier) {
      if (value == null) {
        return null;
      }

      Matcher matcher = pattern.matcher(value);
      while (matcher.find()) {
        String name = matcher.group(1);
        String replace = supplier.apply(name);
        String group = matcher.group(0);
        if (group.equals(value)) {
          return replace;
        }
        if (replace == null) {
          replace = "";
        }
        Pattern subPattern = Pattern.compile(Pattern.quote(group));
        value = subPattern.matcher(value).replaceAll(replace);
      }
      return value;
    }
  }
}
