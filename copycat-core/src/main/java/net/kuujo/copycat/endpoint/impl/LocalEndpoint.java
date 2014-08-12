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
package net.kuujo.copycat.endpoint.impl;

import java.util.Map;

import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.Endpoint;

/**
 * Direct endpoint implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalEndpoint implements Endpoint {
  private CopyCatContext context;

  @Override
  public void init(CopyCatContext context) {
    this.context = context;
  }

  @Override
  public void start(AsyncCallback<Void> callback) {
  }

  @Override
  public void stop(AsyncCallback<Void> callback) {
  }

  /**
   * Submits a command via the endpoint.
   *
   * @param command The command to submit.
   * @param args The command arguments.
   * @param callback An asynchronous callback to be called once complete.
   * @return
   */
  public LocalEndpoint submitCommand(String command, Map<String, Object> args, AsyncCallback<Object> callback) {
    context.submitCommand(command, args, callback);
    return this;
  }

}
