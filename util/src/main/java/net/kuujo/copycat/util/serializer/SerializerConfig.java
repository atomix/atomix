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

import net.kuujo.copycat.util.AbstractConfigurable;

import java.util.Map;

/**
 * Base type for configurable serializers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class SerializerConfig extends AbstractConfigurable implements Serializer {
  private static final String DEFAULT_CONFIGURATION = "serializer-defaults";
  private static final String CONFIGURATION = "serializer";

  protected SerializerConfig() {
    super(DEFAULT_CONFIGURATION, CONFIGURATION);
  }

  protected SerializerConfig(Map<String, Object> config, String... resources) {
    super(config, addResources(resources, DEFAULT_CONFIGURATION, CONFIGURATION));
  }

  protected SerializerConfig(String... resources) {
    super(addResources(resources, DEFAULT_CONFIGURATION, CONFIGURATION));
  }

  protected SerializerConfig(SerializerConfig config) {
    super(config);
  }

  @Override
  public Serializer copy() {
    return (Serializer) super.copy();
  }

}
