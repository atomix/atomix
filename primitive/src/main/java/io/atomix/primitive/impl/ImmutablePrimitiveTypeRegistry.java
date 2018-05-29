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
package io.atomix.primitive.impl;

import com.google.common.collect.ImmutableList;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;

import java.util.Collection;

/**
 * Immutable primitive type registry.
 */
public class ImmutablePrimitiveTypeRegistry implements PrimitiveTypeRegistry {
  private final PrimitiveTypeRegistry primitiveTypes;

  public ImmutablePrimitiveTypeRegistry(PrimitiveTypeRegistry primitiveTypes) {
    this.primitiveTypes = primitiveTypes;
  }

  @Override
  public void addPrimitiveType(PrimitiveType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removePrimitiveType(PrimitiveType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<PrimitiveType> getPrimitiveTypes() {
    return ImmutableList.copyOf(primitiveTypes.getPrimitiveTypes());
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    return primitiveTypes.getPrimitiveType(typeName);
  }
}
