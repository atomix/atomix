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
package net.kuujo.copycat;

/**
 * Interface for providing command descriptions.<p>
 *
 * State machines which implement this interface can support typed
 * commands which allow CopyCat to improve performance by supporting
 * read-only operations without requiring log replication. Command
 * providers should use the {@link GenericCommandInfo} helper class
 * to provide command info for arbitrary named commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CommandProvider {

  /**
   * Returns command info for a named command.
   *
   * @param name The name of the command for which to return info.
   * @return The command info.
   */
  Command getCommandInfo(String name);

}
