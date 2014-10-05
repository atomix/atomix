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
package net.kuujo.copycat.protocol;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.util.List;

import net.kuujo.copycat.log.Entry;

/**
 * Protocol reader helper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ProtocolReader {
  private static final byte PING_REQUEST = 0;
  private static final byte PING_RESPONSE = 1;
  private static final byte SYNC_REQUEST = 2;
  private static final byte SYNC_RESPONSE = 3;
  private static final byte POLL_REQUEST = 4;
  private static final byte POLL_RESPONSE = 5;
  private static final byte SUBMIT_REQUEST = 6;
  private static final byte SUBMIT_RESPONSE = 7;

  /**
   * Reads a request from a byte array.
   *
   * @param bytes The request bytes.
   * @return The request instance.
   */
  @SuppressWarnings("unchecked")
  public <T extends Request> T readRequest(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte type = buffer.get();
    switch (type) {
      case PING_REQUEST:
        return (T) pingRequest(buffer);
      case SYNC_REQUEST:
        return (T) syncRequest(buffer);
      case POLL_REQUEST:
        return (T) pollRequest(buffer);
      case SUBMIT_REQUEST:
        return (T) submitRequest(buffer);
    }
    throw new RuntimeException("Invalid request type");
  }

  /**
   * Reads an ping request.
   */
  private PingRequest pingRequest(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    long term = buffer.getLong();
    int leaderLength = buffer.getInt();
    byte[] leaderBytes = new byte[leaderLength];
    buffer.get(leaderBytes);
    String leader = new String(leaderBytes);
    long logIndex = buffer.getLong();
    long logTerm = buffer.getLong();
    long commitIndex = buffer.getLong();
    return new PingRequest(id, term, leader, logIndex, logTerm, commitIndex);
  }

  /**
   * Reads an append entries request.
   */
  private SyncRequest syncRequest(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    long term = buffer.getLong();
    int leaderLength = buffer.getInt();
    byte[] leaderBytes = new byte[leaderLength];
    buffer.get(leaderBytes);
    String leader = new String(leaderBytes);
    long prevLogIndex = buffer.getLong();
    long prevLogTerm = buffer.getLong();
    int entriesLength = buffer.getInt();
    byte[] entriesBytes = new byte[entriesLength];
    buffer.get(entriesBytes);
    List<Entry> entries = deserializeObject(entriesBytes);
    long commitIndex = buffer.getLong();
    return new SyncRequest(id, term, leader, prevLogIndex, prevLogTerm, entries, commitIndex);
  }

  /**
   * Reads a request vote request.
   */
  private PollRequest pollRequest(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    long term = buffer.getLong();
    int candidateLength = buffer.getInt();
    byte[] candidateBytes = new byte[candidateLength];
    buffer.get(candidateBytes);
    String candidate = new String(candidateBytes);
    long lastLogIndex = buffer.getLong();
    long lastLogTerm = buffer.getLong();
    return new PollRequest(id, term, candidate, lastLogIndex, lastLogTerm);
  }

  /**
   * Reads a submit command request.
   */
  private SubmitRequest submitRequest(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    int commandLength = buffer.getInt();
    byte[] commandBytes = new byte[commandLength];
    buffer.get(commandBytes);
    String command = new String(commandBytes);
    int argsLength = buffer.getInt();
    byte[] argsBytes = new byte[argsLength];
    buffer.get(argsBytes);
    List<Object> args = deserializeObject(argsBytes);
    return new SubmitRequest(id, command, args);
  }

  /**
   * Reads a response from a byte array.
   *
   * @param bytes The response bytes.
   * @return The response instance.
   */
  @SuppressWarnings("unchecked")
  public <T extends Response> T readResponse(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte type = buffer.get();
    switch (type) {
      case PING_RESPONSE:
        return (T) pingResponse(buffer);
      case SYNC_RESPONSE:
        return (T) syncResponse(buffer);
      case POLL_RESPONSE:
        return (T) pollResponse(buffer);
      case SUBMIT_RESPONSE:
        return (T) submitResponse(buffer);
    }
    throw new RuntimeException("Invalid response type");
  }

  /**
   * Reads a ping response.
   */
  private PingResponse pingResponse(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    long term = buffer.getLong();
    boolean succeeded = buffer.getInt() == 1;
    return new PingResponse(id, term, succeeded);
  }

  /**
   * Reads a sync response.
   */
  private SyncResponse syncResponse(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    long term = buffer.getLong();
    boolean succeeded = buffer.getInt() == 1;
    long lastLogIndex = buffer.getLong();
    return new SyncResponse(id, term, succeeded, lastLogIndex);
  }

  /**
   * Reads a request vote response.
   */
  private PollResponse pollResponse(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    long term = buffer.getLong();
    boolean voteGranted = buffer.getInt() == 1;
    return new PollResponse(id, term, voteGranted);
  }

  /**
   * Reads a submit command response.
   */
  private SubmitResponse submitResponse(ByteBuffer buffer) {
    int idLength = buffer.getInt();
    byte[] idBytes = new byte[idLength];
    buffer.get(idBytes);
    Object id = deserializeObject(idBytes);
    int resultLength = buffer.getInt();
    byte[] resultBytes = new byte[resultLength];
    buffer.get(resultBytes);
    Object result = deserializeObject(resultBytes);
    return new SubmitResponse(id, result);
  }

  /**
   * Deserializes a serializeable object.
   */
  @SuppressWarnings("unchecked")
  private <T> T deserializeObject(byte[] bytes) {
    ObjectInputStream stream = null;
    try {
      stream = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), new ByteArrayInputStream(bytes));
      return (T) stream.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
        }
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
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
