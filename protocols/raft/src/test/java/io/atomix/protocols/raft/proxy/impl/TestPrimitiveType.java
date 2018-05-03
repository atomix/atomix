/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

/**
 * Test primitive type.
 */
public class TestPrimitiveType implements PrimitiveType {
  @Override
  public String id() {
    return "test";
  }

  @Override
  public PrimitiveService newService(ServiceConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedPrimitiveBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DistributedPrimitiveBuilder newPrimitiveBuilder(String name, PrimitiveConfig config, PrimitiveManagementService managementService) {
    throw new UnsupportedOperationException();
  }
}
