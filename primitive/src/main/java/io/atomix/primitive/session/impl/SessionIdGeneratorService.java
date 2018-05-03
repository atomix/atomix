/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.primitive.session.impl;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

/**
 * ID generator service.
 */
public class SessionIdGeneratorService extends AbstractPrimitiveService {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(SessionIdGeneratorOperations.NAMESPACE)
      .build());

  private long id;

  public SessionIdGeneratorService(ServiceConfig config) {
    super(config);
  }

  @Override
  public Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeLong(id);
  }

  @Override
  public void restore(BackupInput reader) {
    id = reader.readLong();
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    executor.register(SessionIdGeneratorOperations.NEXT, this::next);
  }

  /**
   * Returns the next session ID.
   *
   * @return the next session ID
   */
  protected long next() {
    return ++id;
  }
}