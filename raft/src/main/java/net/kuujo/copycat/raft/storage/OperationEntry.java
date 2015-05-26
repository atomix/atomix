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
 * limitations under the License.
 */
package net.kuujo.copycat.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.Operation;

/**
 * Operation entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=1008)
public class OperationEntry extends SessionEntry<OperationEntry> {
  private long request;
  private long response;
  private Operation operation;

  public OperationEntry() {
  }

  public OperationEntry(long index) {
    setIndex(index);
  }

  public OperationEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the operation request number.
   *
   * @return The operation request number.
   */
  public long getRequest() {
    return request;
  }

  /**
   * Sets the operation request number.
   *
   * @param request The operation request number.
   * @return The operation entry.
   */
  public OperationEntry setRequest(long request) {
    this.request = request;
    return this;
  }

  /**
   * Returns the operation response number.
   *
   * @return The operation response number.
   */
  public long getResponse() {
    return response;
  }

  /**
   * Sets the operation response number.
   *
   * @param response The operation response number.
   * @return The operation entry.
   */
  public OperationEntry setResponse(long response) {
    this.response = response;
    return this;
  }

  /**
   * Returns the operation.
   *
   * @return The operation.
   */
  public Operation getOperation() {
    return operation;
  }

  /**
   * Sets the operation.
   *
   * @param operation The operation.
   * @return The operation entry.
   */
  public OperationEntry setOperation(Operation operation) {
    this.operation = operation;
    return this;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(request).writeLong(response);
    serializer.writeObject(operation, buffer);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    request = buffer.readLong();
    response = buffer.readLong();
    operation = serializer.readObject(buffer);
  }

}
