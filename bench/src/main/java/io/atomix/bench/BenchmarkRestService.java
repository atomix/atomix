/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixRegistry;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.rest.impl.ConfigPropertyNamingStrategy;
import io.atomix.rest.impl.PartitionGroupDeserializer;
import io.atomix.rest.impl.PolymorphicTypeDeserializer;
import io.atomix.rest.impl.PrimitiveConfigDeserializer;
import io.atomix.rest.impl.PrimitiveProtocolDeserializer;
import io.atomix.rest.impl.VertxRestService;
import io.atomix.utils.net.Address;

/**
 * Benchmark REST API service.
 */
public class BenchmarkRestService extends VertxRestService {
  private final Atomix atomix;

  public BenchmarkRestService(Atomix atomix, Address address) {
    super(atomix, address);
    this.atomix = atomix;
  }

  @Override
  protected ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();

    mapper.setPropertyNamingStrategy(new ConfigPropertyNamingStrategy());
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);

    SimpleModule module = new SimpleModule("PolymorphicTypes");
    module.addDeserializer(PartitionGroupConfig.class, new PartitionGroupDeserializer(atomix.getRegistry()));
    module.addDeserializer(PrimitiveProtocolConfig.class, new PrimitiveProtocolDeserializer(atomix.getRegistry()));
    module.addDeserializer(PrimitiveConfig.class, new PrimitiveConfigDeserializer(atomix.getRegistry()));
    module.addDeserializer(BenchmarkConfig.class, new BenchmarkConfigDeserializer(atomix.getRegistry()));
    mapper.registerModule(module);

    return mapper;
  }

  public class BenchmarkConfigDeserializer extends PolymorphicTypeDeserializer<BenchmarkConfig> {
    @SuppressWarnings("unchecked")
    public BenchmarkConfigDeserializer(AtomixRegistry registry) {
      super(BenchmarkConfig.class, type -> (Class) registry.getType(BenchmarkType.class, type).newConfig().getClass());
    }
  }
}
