/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Command;

/**
 * Resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ResourceCommand<T extends Command<U>, U> extends ResourceOperation<T, U> implements Command<U> {
  private ConsistencyLevel consistency;

  public ResourceCommand() {
  }

  public ResourceCommand(T command, ConsistencyLevel consistency) {
    super(command);
    this.consistency = consistency;
  }

  @Override
  public ConsistencyLevel consistency() {
    ConsistencyLevel consistency = operation.consistency();
    return consistency != null ? consistency : this.consistency;
  }

  @Override
  public CompactionMode compaction() {
    return operation.compaction();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeByte(consistency.ordinal());
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    consistency = ConsistencyLevel.values()[buffer.readByte()];
  }

  @Override
  public String toString() {
    return String.format("%s[command=%s, consistency=%s]", getClass().getSimpleName(), operation, consistency());
  }

}
