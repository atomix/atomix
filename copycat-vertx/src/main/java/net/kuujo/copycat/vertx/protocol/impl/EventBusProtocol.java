package net.kuujo.copycat.vertx.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;

public class EventBusProtocol implements Protocol {
  private EventBusProtocolServer server;
  private EventBusProtocolClient client;

  @Override
  public void init(String address, CopyCatContext context) {
    server = new EventBusProtocolServer(address, vertx);
    client = new EventBusProtocolClient(address, vertx);
  }

  @Override
  public ProtocolServer server() {
    return server;
  }

  @Override
  public ProtocolClient client() {
    return client;
  }

}
