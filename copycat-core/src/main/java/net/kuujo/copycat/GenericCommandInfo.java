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

import java.lang.annotation.Annotation;

/**
 * Generic command info implementation.<p>
 *
 * This class can be used by {@link StateMachine} implementations to provide
 * command info for state machine commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SuppressWarnings("all")
public class GenericCommandInfo implements Annotation, CommandInfo {
  private final String name;
  private final CommandInfo.Type type;

  public GenericCommandInfo(String name, CommandInfo.Type type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CommandInfo.Type type() {
    return type;
  }

  @Override
  public Class<? extends Annotation> annotationType() {
    return CommandInfo.class;
  }
}
