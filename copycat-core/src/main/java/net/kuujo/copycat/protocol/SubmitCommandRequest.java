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

import java.util.List;

/**
 * Submit command request.<p>
 *
 * Submit requests are simply command submissions that are forwarded
 * to the cluster leader. When a node receives a command submission,
 * it can optionally reject the command or forward it using this request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitCommandRequest extends Request {
  private static final long serialVersionUID = -8657438748181101192L;
  private String command;
  private List<Object> args;

  public SubmitCommandRequest() {
    super(null);
  }

  public SubmitCommandRequest(Object id, String command, List<Object> args) {
    super(id);
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
  public List<Object> args() {
    return args;
  }

  @Override
  public String toString() {
    return String.format("%s[command=%s, args=%s]", getClass().getSimpleName(), command, args);
  }

}
