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

import net.kuujo.copycat.Arguments;

/**
 * Submit request.<p>
 *
 * Submit requests are simply command submissions that are forwarded
 * to the cluster leader. When a node receives a command submission,
 * it can optionally reject the command or forward it using this request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitRequest implements Request {
  private static final long serialVersionUID = -8657438748181101192L;
  private String command;
  private Arguments args;

  public SubmitRequest() {
  }

  public SubmitRequest(String command, Arguments args) {
    this.command = command;
    this.args = args;
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
  public Arguments args() {
    return args;
  }

  @Override
  public String toString() {
    return String.format("SubmitRequest[command=%s, args=%s]", command, args);
  }

}
