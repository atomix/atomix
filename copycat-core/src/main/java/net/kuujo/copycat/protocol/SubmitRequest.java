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

import java.util.Map;

import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * A submit request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitRequest extends Request<SubmitResponse> {
  private static final long serialVersionUID = -8657438748181101192L;
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private String command;
  private Map<String, Object> args;

  public SubmitRequest() {
  }

  public SubmitRequest(String command, Map<String, Object> args) {
    this.command = command;
    this.args = args;
  }

  public static SubmitRequest fromJson(byte[] json) {
    return serializer.readValue(json, SubmitRequest.class);
  }

  public static SubmitRequest fromJson(byte[] json, AsyncCallback<SubmitResponse> callback) {
    SubmitRequest request = serializer.readValue(json, SubmitRequest.class);
    request.setResponseCallback(callback);
    return request;
  }

  public static byte[] toJson(SubmitRequest request) {
    return serializer.writeValue(request);
  }

  /**
   * Returns the request command.
   *
   * @return The command being submitted.
   */
  public String command() {
    return command;
  }

  /**
   * Returns the request arguments.
   *
   * @return The arguments to apply to the command being submitted.
   */
  public Map<String, Object> args() {
    return args;
  }

  /**
   * Responds to the request.
   *
   * @param result The command execution result.
   */
  public void respond(Map<String, Object> result) {
    super.respond(new SubmitResponse(result));
  }

  /**
   * Responds to the request with an error.
   *
   * @param t The error that occurred.
   */
  public void respond(Throwable t) {
    super.respond(new SubmitResponse(t));
  }

  /**
   * Responds to the request with an error message.
   *
   * @param message The error message.
   */
  public void respond(String message) {
    super.respond(new SubmitResponse(message));
  }

  @Override
  public String toString() {
    return String.format("SubmitRequest[command=%s, args=%s]", command, args);
  }

}
