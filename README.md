vertx-raft
==========

An implementation of the Raft consensus algorithm on the Vert.x event bus.

### Usage

Create a new replication service:

```java
ReplicationService service = new DefaultReplicationService("foo", vertx);
```

Add cluster members to the service:

```java
service.addMember("bar");
service.addMember("baz");
```

Register state machine commands:

```java
// Register a put command.
service.registerCommand("put", Command.Type.WRITE, new Function<Command, JsonObject>() {
  public JsonObject call(Command command) {
    String key = command.data().getString("key");
    Object value = command.data().getValue("value");
    someMap.put(key, value);
    return new JsonObject().putBoolean("result", true);
  }
});

// Register a get command.
service.registerCommand("get", Command.Type.READ, new Function<Command, JsonObject>() {
  public JsonObject call(Command command) {
    String key = command.data().getString("key");
    if (someMap.containsKey(key)) {
      return new JsonObject().putValue("result", someMap.get(key));
    }
    return new JsonObject().putValue("result", null);
  }
});

// Register a remove command.
service.registerCommand("remove", Command.Type.WRITE, new Function<Command, JsonObject>() {
  public JsonObject call(Command command) {
    String key = command.data().getString("key");
    return new JsonObject().putValue("result", someMap.remove(key));
  }
});
```

Start the service:

```java
service.start(new Handler<AsyncResult<Void>>() {
  public void handle(AsyncResult<Void> result) {
    if (result.succeeded()) {
      // Submit commands to the cluster.
      service.submitCommand("put", new JsonObject().putString("key", "foo").putString("value", "Hello world!"), new Handler<AsyncResult<JsonObject>>() {
        public void handle(AsyncResult<JsonObject> result) {
          if (result.succeeded()) {
            // Now read the command back.
            service.submitCommand("get", new JsonObject().putString("key", "foo"), new Handler<AsyncResult<JsonObject>>() {
              public void handle(AsyncResult<JsonObject> result) {
                if (result.succeeded()) {
                  System.out.println(result.result().getString("result")); // Hello world!
                }
              }
            });
          }
        }
      });
    }
  }
});
```
