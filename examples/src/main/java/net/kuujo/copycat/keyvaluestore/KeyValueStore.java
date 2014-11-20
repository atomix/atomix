package net.kuujo.copycat.keyvaluestore;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.TcpMember;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.protocol.NettyTcpProtocol;

public class KeyValueStore implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  public static void main(String[] args) {
    ClusterConfig clusterConfig = new ClusterConfig().withLocalMember(
      new TcpMember("localhost", 8080)).withRemoteMembers(new TcpMember("localhost", 8081),
      new TcpMember("localhost", 8082));

    new Copycat(new KeyValueStore(), new InMemoryLog(), new Cluster(clusterConfig),
      new NettyTcpProtocol()).start();
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
