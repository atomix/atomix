Copycat
=======

### [User Manual](#user-manual)

Copycat is an extensible Java-based implementation of the
[Raft consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf).

The core of Copycat is a framework designed to integrate with any asynchronous framework
or protocol. Copycat provides a simple extensible API that can be used to build a
strongly consistent, *fault/partition tolerant state machine* over any mode of communication.
Copycat's Raft implementation also supports advanced features of the Raft algorithm such as
snapshotting and dynamic cluster configuration changes and provides *additional optimizations
pipelining, fast Kryo-based serialization, memory-mapped file logging, and read-only state queries.*

Copycat is a pluggable framework, providing protocol and service implementations for
various frameworks such as [Netty](http://netty.io) and [Vert.x](http://vertx.io).

**Please note that Copycat is still undergoing heavy development, and until a beta release,
the API is subject to change.**

Copycat *will* be published to Maven Central once these features are complete. Follow
the project for updates!

*Copycat requires Java 8. Though it has been requested, there are currently no imminent
plans for supporting Java 7. Of course, feel free to fork and PR :-)*

User Manual
===========

**Note: Some of this documentation may be inaccurate due to the rapid development currently
taking place on Copycat**

### Configuration
```java
ClusterConfig cluster = new ClusterConfig()
  .withLocalMember("tcp://123.456.789.0")
  .withRemoteMembers("tcp://234.567.890.1", "tcp://345.678.901.2");
Protocol protocol = new NettyTcpProtocol();
Copycat copycat = Copycat.create(cluster, protocol);
```

or...

```
copycat {
  cluster {
    protocol {
      class: net.kuujo.copycat.protocol.netty.NettyTcpProtocol
      tcp-keep-alive: true
    }
    local-member: tcp://123.456.789.0
    members: [
      tcp://123.456.789.0
      tcp://234.567.890.1
      tcp://345.678.901.2
    ]
  }
}
```

or...

```
copycat.cluster.localMember = tcp://123.456.789.0
copycat.cluster.members.1 = tcp://123.456.789.0
copycat.cluster.members.2 = tcp://234.567.890.1
copycat.cluster.members.3 = tcp://345.678.901.2

copycat.protocol.class = net.kuujo.copycat.protocol.netty.NettyTcpProtocol
copycat.protocol.tcp-keep-alive = true
```

### Event log
When a new log is created, if a log with the given resource name already exists, the log
will become a member of the existing cluster, otherwise it will start a new cluster. In this
way, logs are replicated based on the nodes on which they have been created. If multiple instances
of the same resource are created from the same Copycat instance, each instance will essentially
point to the same local replicated log.
```java
Copycat copycat = Copycat.create();

EventLog log = copycat.eventLog("events");

eventLog.consumer(entry -> System.out.println("Got event " + entry));
log.commit("Hello world!").thenRun(() -> {
  log.commit("Hello world again!").thenRun(() -> {
    log.commit("Hello world once more!").thenRun(() -> {
      log.replay().thenRun(() -> {
        log.get(2).whenComplete((entry, error) -> {
          if (error != null) {
            System.out.println(entry);
          }
        });
      });
    });
  });
});
```

```
Got event Hello world!
Got event Hello world again!
Got event Hello world once more!
Got event Hello world!
Got event Hello world again!
Got event Hello world once more!
Hello world again!
```

### State log
The state log is a strongly consistent log that supports snapshots.

```java
Copycat copycat = Copycat.create();

StateLog log = copycat.stateLog("state");

final Map<String, String> map = new HashMap<>();

log.register("put", entry -> map.put(entry.key, entry.value));
log.register("get", entry -> map.get(entry.key));

log.open().thenRun(() -> {
  log.submit("put", "foo", "Hello world!").thenRun(() -> {
    log.submit("get", "foo").thenAccept(result -> {
      System.out.println("foo is " + result);
    });
  });
});
```

### State machine
The state machine is a strongly consistent persistent log.

```java
public interface MyState {

  void put(String key, Object value);

  Object get(String key);

}
```

```java
public interface MyStateProxy {

  CompletableFuture<Void> put(String key, Object value);

  CompletableFuture<Object> get(String key);

}
```

```java
Copycat copycat = Copycat.create();

StateMachine stateMachine = model.stateMachine("state", MyState.class, new MyInitialState());

MyStateProxy asyncProxy = stateMachine.createProxy(MyStateProxy.class);
asyncProxy.put("foo", "Hello world!").thenRun(() -> {
  asyncProxy.get("foo").thenAccept(result -> {
    System.out.println("foo is " + result);
  });
});

MyState syncProxy = stateMachine.createProxy(MyStateProxy.class);
syncProxy.put("foo", "Hello world!");
Object result = syncProxy.get("foo");
System.out.println("foo is " + result);
```

### Consistent distributed data structures
```java
Copycat copcyat = Copycat.create();

AsyncMap<String, String> asyncMap = copycat.getMap("foo");
asyncMap.put("foo", "bar").thenRun(() -> {
  asyncMap.get("foo").thenAccept(result -> {
    System.out.println("foo is " + result);
  });
});

Map<String, String> map = copycat.getSyncMap("foo");
map.put("foo", "bar");
System.out.println("foo is " + map.get("foo"));

AsyncList<String> asyncList = copycat.getList("bar");
asyncList.add("Hello world!").thenRun(() -> {
  asyncList.get(0).thenAccept(result -> {
    System.out.println("list index 0 is " + result);
  });
});

List<String> list = copycat.getSyncList("bar");
list.add("Hello world!");
System.out.println("list index 0 is " + list.get(0));
```

### Messaging
```java
Copycat copycat = Copycat.create();

copycat.cluster().localMember().handler(message -> {
  return CompletableFuture.completedFuture("world!");
});

copycat.cluster().member("tcp://123.456.789.0").send("Hello").thenAccept(response -> {
  System.out.println("Hello " + response);
});
```

### Remote execution
```java
Copycat copycat = Copycat.create();

copycat.cluster.member("tcp://123.456.789.0").execute(() -> {
  System.out.println("I'm running on tcp://123.456.789.0!");
});

copycat.cluster.member("tcp://123.456.789.0").submit(() -> "Hello world!").thenAccept(response -> {
  System.out.println(response);
});
```

### Leader election
```java
Copycat copycat = Copycat.create();

copycat.election("foo").handler(leader -> {
  leader.submit(() -> {
    System.out.println("I'm running on the leader!");
  });
});
```
