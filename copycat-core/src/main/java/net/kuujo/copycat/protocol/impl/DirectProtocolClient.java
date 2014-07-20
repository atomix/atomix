package net.kuujo.copycat.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

public class DirectProtocolClient implements ProtocolClient {
  private final String address;
  private final DirectProtocolRegistry registry;

  public DirectProtocolClient(String address, CopyCatContext context) {
    this.address = address;
    registry = DirectProtocolRegistry.getInstance(context);
  }

  @Override
  public void ping(PingRequest request, AsyncCallback<PingResponse> callback) {
    DirectProtocolServer server = registry.get(address);
    if (server != null) {
      server.ping(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void sync(SyncRequest request, AsyncCallback<SyncResponse> callback) {
    DirectProtocolServer server = registry.get(address);
    if (server != null) {
      server.sync(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void install(InstallRequest request, AsyncCallback<InstallResponse> callback) {
    DirectProtocolServer server = registry.get(address);
    if (server != null) {
      server.install(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void poll(PollRequest request, AsyncCallback<PollResponse> callback) {
    DirectProtocolServer server = registry.get(address);
    if (server != null) {
      server.poll(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void submit(SubmitRequest request, AsyncCallback<SubmitResponse> callback) {
    DirectProtocolServer server = registry.get(address);
    if (server != null) {
      server.submit(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

}
