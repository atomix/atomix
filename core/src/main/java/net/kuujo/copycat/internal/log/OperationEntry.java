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
package net.kuujo.copycat.internal.log;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import net.kuujo.copycat.log.EntryType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * State machine operation entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=3, serializer=OperationEntry.Serializer.class)
public class OperationEntry extends CopycatEntry {
  private String operation;
  private List<Object> args;

  private OperationEntry() {
    super();
  }

  public OperationEntry(long term, String operation, Object... args) {
    this(term, operation, Arrays.asList(args));
  }

  public OperationEntry(long term, String operation, List<Object> args) {
    super(term);
    this.operation = operation;
    this.args = args;
  }

  /**
   * Returns the state machine operation.
   * 
   * @return The state machine operation.
   */
  public String operation() {
    return operation;
  }

  /**
   * Returns command arguments.
   *
   * @return The command arguments.
   */
  public List<Object> args() {
    return args;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OperationEntry) {
      OperationEntry entry = (OperationEntry) object;
      return term == entry.term && operation.equals(entry.operation) && args.equals(entry.args);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    hashCode = 37 * hashCode + operation.hashCode();
    hashCode = 37 * hashCode + args.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("OperationEntry[term=%d, operation=%s, args=%s]", term, operation, args);
  }

  /**
   * Operation entry serializer.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Serializer extends com.esotericsoftware.kryo.Serializer<OperationEntry> {
    @Override
    @SuppressWarnings("unchecked")
    public OperationEntry read(Kryo kryo, Input input, Class<OperationEntry> type) {
      OperationEntry entry = new OperationEntry();
      entry.term = input.readLong();
      int commandLength = input.readInt();
      byte[] commandBytes = new byte[commandLength];
      input.readBytes(commandBytes);
      entry.operation = new String(commandBytes);
      entry.args = kryo.readObject(input, ArrayList.class);
      return entry;
    }
    @Override
    public void write(Kryo kryo, Output output, OperationEntry entry) {
      output.writeLong(entry.term);
      byte[] commandBytes = entry.operation.getBytes();
      output.writeInt(commandBytes.length);
      output.writeBytes(commandBytes);
      kryo.writeObject(output, entry.args);
    }
  }

}
