package net.kuujo.copycat.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;

public class DirectProtocol implements Protocol {
  private ProtocolServer server;
  private ProtocolClient client;

  @Override
  public void init(String address, CopyCatContext context) {
    server = new DirectProtocolServer(address, context);
    client = new DirectProtocolClient(address, context);
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
