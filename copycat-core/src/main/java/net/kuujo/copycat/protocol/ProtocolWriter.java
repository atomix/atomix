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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 * Protocol writer helper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ProtocolWriter {
  private static final byte APPEND_ENTRIES_REQUEST = 0;
  private static final byte APPEND_ENTRIES_RESPONSE = 1;
  private static final byte REQUEST_VOTE_REQUEST = 2;
  private static final byte REQUEST_VOTE_RESPONSE = 3;
  private static final byte SUBMIT_COMMAND_REQUEST = 4;
  private static final byte SUBMIT_COMMAND_RESPONSE = 5;

  /**
   * Writes a request as a byte array.
   *
   * @param request The request to write.
   * @return The written request.
   */
  public byte[] writeRequest(Request request) {
    if (request instanceof AppendEntriesRequest) {
      return appendEntriesRequest((AppendEntriesRequest) request);
    } else if (request instanceof RequestVoteRequest) {
      return requestVoteRequest((RequestVoteRequest) request);
    } else if (request instanceof SubmitCommandRequest) {
      return submitCommandRequest((SubmitCommandRequest) request);
    }
    throw new RuntimeException("Invalid request type");
  }

  /**
   * Writes an append entries request.
   */
  private byte[] appendEntriesRequest(AppendEntriesRequest request) {
    byte[] idBytes = serializeObject(request.id());
    byte[] leaderBytes = request.leader().getBytes();
    byte[] entriesBytes = serializeObject(request.entries());
    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 * 4 + 4 * 3 + idBytes.length + leaderBytes.length + entriesBytes.length);
    buffer.put(APPEND_ENTRIES_REQUEST);
    buffer.putInt(idBytes.length);
    buffer.put(idBytes);
    buffer.putInt(leaderBytes.length);
    buffer.put(leaderBytes);
    buffer.putLong(request.prevLogIndex());
    buffer.putLong(request.prevLogTerm());
    buffer.putInt(entriesBytes.length);
    buffer.put(entriesBytes);
    buffer.putLong(request.commitIndex());
    return buffer.array();
  }

  /**
   * Writes a request vote request.
   */
  private byte[] requestVoteRequest(RequestVoteRequest request) {
    byte[] idBytes = serializeObject(request.id());
    byte[] candidateBytes = request.candidate().getBytes();
    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 * 3 + 4 * 2 + idBytes.length + candidateBytes.length);
    buffer.put(REQUEST_VOTE_REQUEST);
    buffer.putInt(idBytes.length);
    buffer.put(idBytes);
    buffer.putInt(candidateBytes.length);
    buffer.put(candidateBytes);
    buffer.putLong(request.lastLogIndex());
    buffer.putLong(request.lastLogTerm());
    return buffer.array();
  }

  /**
   * Writes a submit command request.
   */
  private byte[] submitCommandRequest(SubmitCommandRequest request) {
    byte[] idBytes = serializeObject(request.id());
    byte[] commandBytes = request.command().getBytes();
    byte[] argsBytes = serializeObject(request.args());
    ByteBuffer buffer = ByteBuffer.allocate(1 + 4 * 3 + idBytes.length + commandBytes.length + argsBytes.length);
    buffer.put(SUBMIT_COMMAND_REQUEST);
    buffer.putInt(idBytes.length);
    buffer.put(idBytes);
    buffer.putInt(commandBytes.length);
    buffer.put(commandBytes);
    buffer.putInt(argsBytes.length);
    buffer.put(argsBytes);
    return buffer.array();
  }

  /**
   * Writes a response as byte array.
   *
   * @param response The response to write.
   * @return The response as a byte array.
   */
  public byte[] writeResponse(Response response) {
    if (response instanceof AppendEntriesResponse) {
      return appendEntriesResponse((AppendEntriesResponse) response);
    } else if (response instanceof RequestVoteResponse) {
      return requestVoteResponse((RequestVoteResponse) response);
    } else if (response instanceof SubmitCommandResponse) {
      return submitCommandResponse((SubmitCommandResponse) response);
    }
    throw new RuntimeException("Invalid response type");
  }

  /**
   * Writes an append entries response.
   */
  private byte[] appendEntriesResponse(AppendEntriesResponse response) {
    byte[] idBytes = serializeObject(response.id());
    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 * 2 + 4 * 2 + idBytes.length);
    buffer.put(APPEND_ENTRIES_RESPONSE);
    buffer.putInt(idBytes.length);
    buffer.put(idBytes);
    buffer.putLong(response.term());
    buffer.putInt(response.succeeded() ? 1 : 0);
    buffer.putLong(response.lastLogIndex());
    return buffer.array();
  }

  /**
   * Writes a request vote response.
   */
  private byte[] requestVoteResponse(RequestVoteResponse response) {
    byte[] idBytes = serializeObject(response.id());
    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 4 * 2 + idBytes.length);
    buffer.put(REQUEST_VOTE_RESPONSE);
    buffer.putInt(idBytes.length);
    buffer.put(idBytes);
    buffer.putLong(response.term());
    buffer.putInt(response.voteGranted() ? 1 : 0);
    return buffer.array();
  }

  /**
   * Writes a submit command response.
   */
  private byte[] submitCommandResponse(SubmitCommandResponse response) {
    byte[] idBytes = serializeObject(response.id());
    byte[] resultBytes = serializeObject(response.result());
    ByteBuffer buffer = ByteBuffer.allocate(1 + 4 * 2 + idBytes.length + resultBytes.length);
    buffer.put(SUBMIT_COMMAND_RESPONSE);
    buffer.putInt(idBytes.length);
    buffer.put(idBytes);
    buffer.putInt(resultBytes.length);
    buffer.put(resultBytes);
    return buffer.array();
  }

  /**
   * Serializes a serializeable object.
   */
  private byte[] serializeObject(Object object) {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ObjectOutputStream stream = null;
    try {
      stream = new ObjectOutputStream(byteStream);
      stream.writeObject(object);
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
    return byteStream.toByteArray();
  }

}
