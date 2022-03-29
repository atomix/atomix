// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session.impl;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

/**
 * ID generator service.
 */
public class SessionIdGeneratorService extends AbstractPrimitiveService {

  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(SessionIdGeneratorOperations.NAMESPACE)
      .build());

  private long id;

  public SessionIdGeneratorService() {
    super(SessionIdGeneratorType.instance());
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
