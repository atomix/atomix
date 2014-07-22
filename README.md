CopyCat
=======
CopyCat is a fault-tolerant state machine replication framework built on Vert.x. It
provides a simple and flexible API built around the Raft consensus algorithm as
described in [the excellent paper by Diego Ongaro and John Ousterhout](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of CopyCat is a framework designed to support a variety of protocols and
transports. CopyCat provides a simple extensible API that can be used to build a
fault-tolerant state machine over TCP, HTTP/REST, messaging systems, or any other
form of communication. The CopyCat Raft implementation supports advanced features
of the Raft algorithm such as snapshotting and dynamic cluster configuration changes.

## Writing a state machine

```java
public class MyStateMachine implements StateMachine {
  private Map<String, Object> data = new HashMap<>();

  @Override
  public Map<String, Object> createSnapshot() {
    return data;
  }

  @Override
  public void installSnapshot(Map<String, Object> data) {
    this.data = data;
  }

  @Override
  public Map<String, Object> applyCommand(String command, Map<String, Object> args) {
    switch (command) {
      case "read":
        read(args);
      case "write":
        return write(args);
      default:
        throw new UnsupportedOperationException();
    }
  }

}
```

## Providing command types

```java
public class MyStateMachine implements StateMachine, CommandProvider {
  private static final CommandInfo READ = new GenericCommandInfo("read", CommandInfo.Type.READ);
  private static final CommandInfo WRITE = new GenericCommandInfo("write", CommandInfo.Type.WRITE);
  private static final CommandInfo NONE = new GenericCommandInfo("none", CommandInfo.Type.READ_WRITE);

  @Override
  public CommandInfo getCommandInfo(String command) {
    switch (command) {
      case "read":
        return READ;
      case "write":
        return WRITE;
      default:
        return NONE;
    }
  }

}
```

## Setting up the cluster

```java
ClusterConfig cluster = new StaticClusterConfig("eventbus:foo");
cluster.setRemoteMembers("eventbus:bar", "eventbus:baz");
```

## Creating the CopyCat context

```java
CopyCatContext context = new CopyCatContext(new MyStateMachine(), cluster);
context.start();
```

## Submitting commands to the cluster

```java
Map<String, Object> args = new HashMap<>();
args.put("key", "foo");
context.submitCommand("read", args, new AsyncCallback<Map<String, Object>>() {
  public void complete(Map<String, Object> result) {
    Object value = result.get("result");
  }
  public void fail(Throwable t) {
    // An error occurred!
  }
});
```

## Using dynamic cluster configuration chagnes

```java
ClusterConfig cluster = new StaticClusterConfig("eventbus:foo");
cluster.addRemoteMember("eventbus:bar");

CopyCatContext context = new CopyCatContext(new MyStateMachine(), cluster);
context.start();

cluster.addRemoteMember("eventbus:baz"); // The cluster change will be logged and replicated
```

## Protocols

## Services

```
Registry registry = new BasicRegistry();
registry.bind("vertx", vertx);

CopyCat copycat = new CopyCat("eventbus:my.service?vertx=#vertx", new MyStateMachine(), cluster, registry);
copycat.start();
```

```java
CopyCat copycat = new CopyCat(new EventBusService("my.service", vertx), new MyStateMachine(), cluster);
copycat.start();
```
