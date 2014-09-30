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
package net.kuujo.copycat.log.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.List;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.EntryReader;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.EntryWriter;

/**
 * State machine command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=1, reader=CommandEntry.Reader.class, writer=CommandEntry.Writer.class)
public class CommandEntry extends RaftEntry {
  private String command;
  private List<Object> args;

  private CommandEntry() {
    super();
  }

  public CommandEntry(long term, String command, List<Object> args) {
    super(term);
    this.command = command;
    this.args = args;
  }

  /**
   * Returns the state machine command.
   * 
   * @return The state machine command.
   */
  public String command() {
    return command;
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
  public String toString() {
    return String.format("CommandEntry[term=%d, command=%s, args=%s]", term, command, args);
  }

  public static class Reader implements EntryReader<CommandEntry> {
    @Override
    @SuppressWarnings("unchecked")
    public CommandEntry readEntry(Buffer buffer) {
      CommandEntry entry = new CommandEntry();
      entry.term = buffer.getLong();
      int commandLength = buffer.getInt();
      byte[] commandBytes = buffer.getBytes(commandLength);
      entry.command = new String(commandBytes);
      int argsLength = buffer.getInt();
      byte[] argsBytes = buffer.getBytes(argsLength);
      ObjectInputStream stream = null;
      try {
        stream = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), new ByteArrayInputStream(argsBytes));
        entry.args = (List<Object>) stream.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      } finally {
        if (stream != null) {
          try {
            stream.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return entry;
    }
  }

  public static class Writer implements EntryWriter<CommandEntry> {
    @Override
    public void writeEntry(CommandEntry entry, Buffer buffer) {
      buffer.appendLong(entry.term);
      byte[] bytes = entry.command.getBytes();
      buffer.appendInt(bytes.length);
      buffer.appendBytes(bytes);
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      ObjectOutputStream stream = null;
      try {
        stream = new ObjectOutputStream(byteStream);
        stream.writeObject(entry.args);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (stream != null) {
          try {
            stream.close();
          } catch (IOException e) {
          }
        }
      }
      byte[] argsBytes = byteStream.toByteArray();
      buffer.appendInt(argsBytes.length);
      buffer.appendBytes(argsBytes);
    }
  }

  /**
   * Object input stream that loads the class from the current context class loader.
   */
  private static class ClassLoaderObjectInputStream extends ObjectInputStream {
    private final ClassLoader cl;

    public ClassLoaderObjectInputStream(ClassLoader cl, InputStream in) throws IOException {
      super(in);
      this.cl = cl;
    }

    @Override
    public Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
      try {
        return cl.loadClass(desc.getName());
      } catch (Exception e) {
      }
      return super.resolveClass(desc);
    }

  }

}
