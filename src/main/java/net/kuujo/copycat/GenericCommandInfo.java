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
