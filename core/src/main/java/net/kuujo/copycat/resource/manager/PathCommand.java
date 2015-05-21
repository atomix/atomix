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
package net.kuujo.copycat.resource.manager;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.resource.Command;

/**
 * Base getPath command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PathCommand<T> extends Command<T> {
  private String path;

  public PathCommand() {
  }

  public PathCommand(String path) {
    this.path = path;
  }

  /**
   * Returns the getPath.
   *
   * @return The getPath.
   */
  public String getPath() {
    return path;
  }

  /**
   * Sets the command path.
   *
   * @param path The command path.
   * @return The command.
   */
  public PathCommand setPath(String path) {
    this.path = path;
    return this;
  }

  @Override
  public void writeObject(Buffer buffer) {
    super.writeObject(buffer);
    buffer.writeInt(path.getBytes().length).write(path.getBytes());
  }

  @Override
  public void readObject(Buffer buffer) {
    super.readObject(buffer);
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    path = new String(bytes);
  }

}
