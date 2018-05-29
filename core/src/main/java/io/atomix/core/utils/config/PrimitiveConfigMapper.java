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
package io.atomix.core.utils.config;

import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveType;
import io.atomix.utils.config.PolymorphicTypeMapper;

/**
 * Primitive configuration mapper.
 */
public class PrimitiveConfigMapper extends PolymorphicTypeMapper<PrimitiveConfig<?>, PrimitiveType> {
  public PrimitiveConfigMapper() {
    super(PrimitiveConfig.class, PrimitiveType.class);
  }

  @Override
  public String getTypePath(String typeName) {
    return String.format("registry.primitiveTypes.%s", typeName);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<? extends PrimitiveConfig<?>> getConcreteTypedClass(PrimitiveType type) {
    return type.configClass();
  }
}
