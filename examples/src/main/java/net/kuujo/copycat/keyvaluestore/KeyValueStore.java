package net.kuujo.copycat.keyvaluestore;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalClusterConfig;
import net.kuujo.copycat.log.InMemoryLog;

public class KeyValueStore implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  public static void main(String[] args) {
    LocalClusterConfig clusterConfig = new LocalClusterConfig();
    clusterConfig.setLocalMember("tcp://localhost:8080");
    clusterConfig.setRemoteMembers("tcp://localhost:8081", "tcp://localhost:8082");

    Copycat.builder()
      .withStateMachine(new KeyValueStore())
      .withLog(new InMemoryLog())
      .withCluster(new Cluster(clusterConfig))
      .build()
      .start();
  }

  @Query
  public Object get(String key) {
    return data.get(key);
  }

  @Command
  public void set(String key, Object value) {
    data.put(key, value);
  }

  @Command
  public void delete(String key) {
    data.remove(key);
  }
}
