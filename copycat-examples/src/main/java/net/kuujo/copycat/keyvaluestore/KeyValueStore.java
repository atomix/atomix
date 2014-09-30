package net.kuujo.copycat.keyvaluestore;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.CopyCat;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.LogFactory;
import net.kuujo.copycat.log.impl.FileLogFactory;

public class KeyValueStore extends StateMachine {
  public static void main(String[] args) {
    // Create the local file log.
    LogFactory logFactory = new FileLogFactory();

    // Configure the cluster.
    ClusterConfig cluster = new ClusterConfig();
    cluster.setLocalMember("tcp://localhost:8080");
    cluster.setRemoteMembers("tcp://localhost:8081", "tcp://localhost:8082");

    // Create and start a server at localhost:5000.
    new CopyCat("http://localhost:5000", new KeyValueStore(), logFactory, cluster).start();
  }

  @Stateful
  private Map<String, Object> data = new HashMap<>();

  @Command(type = Command.Type.READ)
  public Object get(String key) {
    return data.get(key);
  }

  @Command(type = Command.Type.WRITE)
  public void set(String key, Object value) {
    data.put(key, value);
  }

  @Command(type = Command.Type.WRITE)
  public void delete(String key) {
    data.remove(key);
  }
}
