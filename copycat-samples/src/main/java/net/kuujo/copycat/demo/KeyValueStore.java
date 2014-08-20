package net.kuujo.copycat.demo;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.AnnotatedStateMachine;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Command.Argument;
import net.kuujo.copycat.Stateful;

public class KeyValueStore extends AnnotatedStateMachine {

  @Stateful
  private final Map<String, Object> data = new HashMap<>();
  
  @Command(name="get", type=Command.Type.READ)
  public Object get(@Argument("key") String key) {
    return data.get(key);
  }

  @Command(name="set", type=Command.Type.WRITE)
  public void set(@Argument("key") String key, @Argument("value") Object value) {
    data.put(key, value);
  }

  @Command(name="delete", type=Command.Type.WRITE)
  public void delete(@Argument("key") String key) {
    data.remove(key);
  }

}