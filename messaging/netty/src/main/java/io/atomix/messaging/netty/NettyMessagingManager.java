/*
 * Copyright 2015-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.messaging.netty;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingException;
import io.atomix.messaging.MessagingService;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.net.ConnectException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Netty based MessagingService.
 */
public class NettyMessagingManager implements MessagingService {
  private static final String DEFAULT_NAME = "atomix";
  private static final long DEFAULT_TIMEOUT_MILLIS = 500;
  private static final long HISTORY_EXPIRE_MILLIS = Duration.ofMinutes(10).toMillis();
  private static final long MIN_TIMEOUT_MILLIS = 100;
  private static final long MAX_TIMEOUT_MILLIS = 5000;
  private static final long TIMEOUT_INTERVAL = 50;
  private static final int WINDOW_SIZE = 100;
  private static final double TIMEOUT_MULTIPLIER = 2.5;
  private static final short MIN_KS_LENGTH = 6;
  private static final int CHANNEL_POOL_SIZE = 8;

  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final ClientConnection localClientConnection = new LocalClientConnection();
  private final ServerConnection localServerConnection = new LocalServerConnection(null);

  private final Endpoint localEndpoint;
  private final int preamble;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Map<String, BiConsumer<InternalMessage, ServerConnection>> handlers = new ConcurrentHashMap<>();
  private final Map<Channel, RemoteClientConnection> clientConnections = Maps.newConcurrentMap();
  private final Map<Channel, RemoteServerConnection> serverConnections = Maps.newConcurrentMap();
  private final AtomicLong messageIdGenerator = new AtomicLong(0);

  private ScheduledFuture<?> timeoutFuture;

  private final Map<Endpoint, List<CompletableFuture<Channel>>> channels = Maps.newConcurrentMap();

  private EventLoopGroup serverGroup;
  private EventLoopGroup clientGroup;
  private Class<? extends ServerChannel> serverChannelClass;
  private Class<? extends Channel> clientChannelClass;
  private ScheduledExecutorService timeoutExecutor;

  protected static final boolean TLS_DISABLED = false;
  protected boolean enableNettyTls = TLS_DISABLED;

  protected String ksLocation;
  protected String tsLocation;
  protected char[] ksPwd;
  protected char[] tsPwd;

  public NettyMessagingManager(Endpoint localEndpoint) {
    this(DEFAULT_NAME, localEndpoint);
  }

  public NettyMessagingManager(String name, Endpoint localEndpoint) {
    this.preamble = name.hashCode();
    this.localEndpoint = checkNotNull(localEndpoint, "localEndpoint cannot be null");

    try {
      activate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Activates the messaging service.
   */
  private void activate() throws Exception {
    getTlsParameters();
    if (started.get()) {
      log.warn("Already running at local endpoint: {}", localEndpoint);
      return;
    }
    initEventLoopGroup();
    startAcceptingConnections();
    timeoutExecutor = Executors.newSingleThreadScheduledExecutor(
        namedThreads("atomix-messaging-timeout-%d", log));
    timeoutFuture = timeoutExecutor.scheduleAtFixedRate(
        this::timeoutAllCallbacks, TIMEOUT_INTERVAL, TIMEOUT_INTERVAL, TimeUnit.MILLISECONDS);
    started.set(true);
    log.info("Started");
  }

  /**
   * Closes the messaging service.
   */
  public void close() {
    if (started.get()) {
      serverGroup.shutdownGracefully();
      clientGroup.shutdownGracefully();
      timeoutFuture.cancel(false);
      timeoutExecutor.shutdown();
      started.set(false);
    }
    log.info("Stopped");
  }

  private void getTlsParameters() {
    String tempString = System.getProperty("enableNettyTLS");
    enableNettyTls = Strings.isNullOrEmpty(tempString) ? TLS_DISABLED : Boolean.parseBoolean(tempString);
    log.info("enableNettyTLS = {}", enableNettyTls);
    if (enableNettyTls) {
      ksLocation = System.getProperty("javax.net.ssl.keyStore");
      if (Strings.isNullOrEmpty(ksLocation)) {
        enableNettyTls = TLS_DISABLED;
        return;
      }
      tsLocation = System.getProperty("javax.net.ssl.trustStore");
      if (Strings.isNullOrEmpty(tsLocation)) {
        enableNettyTls = TLS_DISABLED;
        return;
      }
      ksPwd = System.getProperty("javax.net.ssl.keyStorePassword").toCharArray();
      if (MIN_KS_LENGTH > ksPwd.length) {
        enableNettyTls = TLS_DISABLED;
        return;
      }
      tsPwd = System.getProperty("javax.net.ssl.trustStorePassword").toCharArray();
      if (MIN_KS_LENGTH > tsPwd.length) {
        enableNettyTls = TLS_DISABLED;
        return;
      }
    }
  }

  private void initEventLoopGroup() {
    // try Epoll first and if that does work, use nio.
    try {
      clientGroup = new EpollEventLoopGroup(0, namedThreads("atomix-messaging-client-event-epoll-%d", log));
      serverGroup = new EpollEventLoopGroup(0, namedThreads("atomix-messaging-server-event-epoll-%d", log));
      serverChannelClass = EpollServerSocketChannel.class;
      clientChannelClass = EpollSocketChannel.class;
      return;
    } catch (Throwable e) {
      log.debug("Failed to initialize native (epoll) transport. "
          + "Reason: {}. Proceeding with nio.", e.getMessage());
    }
    clientGroup = new NioEventLoopGroup(0, namedThreads("atomix-messaging-client-event-nio-%d", log));
    serverGroup = new NioEventLoopGroup(0, namedThreads("atomix-messaging-server-event-nio-%d", log));
    serverChannelClass = NioServerSocketChannel.class;
    clientChannelClass = NioSocketChannel.class;
  }

  /**
   * Times out response callbacks.
   */
  private void timeoutAllCallbacks() {
    // Iterate through all connections and time out callbacks.
    for (RemoteClientConnection connection : clientConnections.values()) {
      connection.timeoutCallbacks();
    }
  }

  @Override
  public CompletableFuture<Void> sendAsync(Endpoint ep, String type, byte[] payload) {
    InternalMessage message = new InternalMessage(preamble,
        messageIdGenerator.incrementAndGet(),
        localEndpoint,
        type,
        payload);
    return executeOnPooledConnection(ep, type, c -> c.sendAsync(message), MoreExecutors.directExecutor());
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload) {
    return sendAndReceive(ep, type, payload, MoreExecutors.directExecutor());
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload, Executor executor) {
    Long messageId = messageIdGenerator.incrementAndGet();
    InternalMessage message = new InternalMessage(preamble,
        messageId,
        localEndpoint,
        type,
        payload);
    return executeOnPooledConnection(ep, type, c -> c.sendAndReceive(message), executor);
  }

  private List<CompletableFuture<Channel>> getChannelPool(Endpoint endpoint) {
    return channels.computeIfAbsent(endpoint, e -> {
      List<CompletableFuture<Channel>> defaultList = new ArrayList<>(CHANNEL_POOL_SIZE);
      for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
        defaultList.add(null);
      }
      return Lists.newCopyOnWriteArrayList(defaultList);
    });
  }

  private int getChannelOffset(String messageType) {
    return Math.abs(messageType.hashCode() % CHANNEL_POOL_SIZE);
  }

  private CompletableFuture<Channel> getChannel(Endpoint endpoint, String messageType) {
    List<CompletableFuture<Channel>> channelPool = getChannelPool(endpoint);
    int offset = getChannelOffset(messageType);

    CompletableFuture<Channel> channelFuture = channelPool.get(offset);
    if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
      synchronized (channelPool) {
        channelFuture = channelPool.get(offset);
        if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
          channelFuture = openChannel(endpoint);
          channelPool.set(offset, channelFuture);
        }
      }
    }

    CompletableFuture<Channel> future = new CompletableFuture<>();
    final CompletableFuture<Channel> finalFuture = channelFuture;
    finalFuture.whenComplete((channel, error) -> {
      if (error == null) {
        if (!channel.isActive()) {
          synchronized (channelPool) {
            CompletableFuture<Channel> currentFuture = channelPool.get(offset);
            if (currentFuture == finalFuture) {
              channelPool.set(offset, null);
              getChannel(endpoint, messageType).whenComplete((recursiveResult, recursiveError) -> {
                if (recursiveError == null) {
                  future.complete(recursiveResult);
                } else {
                  future.completeExceptionally(recursiveError);
                }
              });
            } else {
              currentFuture.whenComplete((recursiveResult, recursiveError) -> {
                if (recursiveError == null) {
                  future.complete(recursiveResult);
                } else {
                  future.completeExceptionally(recursiveError);
                }
              });
            }
          }
        } else {
          future.complete(channel);
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  private <T> CompletableFuture<T> executeOnPooledConnection(
      Endpoint endpoint,
      String type,
      Function<ClientConnection, CompletableFuture<T>> callback,
      Executor executor) {
    CompletableFuture<T> future = new CompletableFuture<T>();
    executeOnPooledConnection(endpoint, type, callback, executor, future);
    return future;
  }

  private <T> void executeOnPooledConnection(
      Endpoint endpoint,
      String type,
      Function<ClientConnection, CompletableFuture<T>> callback,
      Executor executor,
      CompletableFuture<T> future) {
    if (endpoint.equals(localEndpoint)) {
      callback.apply(localClientConnection).whenComplete((result, error) -> {
        if (error == null) {
          executor.execute(() -> future.complete(result));
        } else {
          executor.execute(() -> future.completeExceptionally(error));
        }
      });
      return;
    }

    getChannel(endpoint, type).whenComplete((channel, channelError) -> {
      if (channelError == null) {
        ClientConnection connection = clientConnections.computeIfAbsent(channel, RemoteClientConnection::new);
        callback.apply(connection).whenComplete((result, sendError) -> {
          if (sendError == null) {
            executor.execute(() -> future.complete(result));
          } else {
            executor.execute(() -> future.completeExceptionally(sendError));
          }
        });
      } else {
        executor.execute(() -> future.completeExceptionally(channelError));
      }
    });
  }

  @Override
  public void registerHandler(String type, BiConsumer<Endpoint, byte[]> handler, Executor executor) {
    handlers.put(type, (message, connection) -> executor.execute(() ->
        handler.accept(message.sender(), message.payload())));
  }

  @Override
  public void registerHandler(String type, BiFunction<Endpoint, byte[], byte[]> handler, Executor executor) {
    handlers.put(type, (message, connection) -> executor.execute(() -> {
      byte[] responsePayload = null;
      InternalMessage.Status status = InternalMessage.Status.OK;
      try {
        responsePayload = handler.apply(message.sender(), message.payload());
      } catch (Exception e) {
        status = InternalMessage.Status.ERROR_HANDLER_EXCEPTION;
      }
      connection.reply(message, status, Optional.ofNullable(responsePayload));
    }));
  }

  @Override
  public void registerHandler(String type, BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler) {
    handlers.put(type, (message, connection) -> {
      handler.apply(message.sender(), message.payload()).whenComplete((result, error) -> {
        InternalMessage.Status status = error == null ? InternalMessage.Status.OK : InternalMessage.Status.ERROR_HANDLER_EXCEPTION;
        connection.reply(message, status, Optional.ofNullable(result));
      });
    });
  }

  @Override
  public void unregisterHandler(String type) {
    handlers.remove(type);
  }

  private Bootstrap bootstrapClient(Endpoint endpoint) {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
        new WriteBufferWaterMark(10 * 32 * 1024, 10 * 64 * 1024));
    bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
    bootstrap.group(clientGroup);
    // TODO: Make this faster:
    // http://normanmaurer.me/presentations/2014-facebook-eng-netty/slides.html#37.0
    bootstrap.channel(clientChannelClass);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.remoteAddress(endpoint.host(), endpoint.port());
    if (enableNettyTls) {
      bootstrap.handler(new SslClientCommunicationChannelInitializer());
    } else {
      bootstrap.handler(new BasicChannelInitializer());
    }
    return bootstrap;
  }

  private void startAcceptingConnections() throws InterruptedException {
    ServerBootstrap b = new ServerBootstrap();
    b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
        new WriteBufferWaterMark(8 * 1024, 32 * 1024));
    b.option(ChannelOption.SO_RCVBUF, 1048576);
    b.option(ChannelOption.TCP_NODELAY, true);
    b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    b.group(serverGroup, clientGroup);
    b.channel(serverChannelClass);
    if (enableNettyTls) {
      b.childHandler(new SslServerCommunicationChannelInitializer());
    } else {
      b.childHandler(new BasicChannelInitializer());
    }
    b.option(ChannelOption.SO_BACKLOG, 128);
    b.childOption(ChannelOption.SO_KEEPALIVE, true);

    // Bind and start to accept incoming connections.
    b.bind(localEndpoint.port()).sync().addListener(future -> {
      if (future.isSuccess()) {
        log.info("{} accepting incoming connections on port {}",
            localEndpoint.host(), localEndpoint.port());
      } else {
        log.warn("{} failed to bind to port {} due to {}",
            localEndpoint.host(), localEndpoint.port(), future.cause());
      }
    });
  }

  private CompletableFuture<Channel> openChannel(Endpoint ep) {
    Bootstrap bootstrap = bootstrapClient(ep);
    CompletableFuture<Channel> retFuture = new CompletableFuture<>();
    ChannelFuture f = bootstrap.connect();

    f.addListener(future -> {
      if (future.isSuccess()) {
        retFuture.complete(f.channel());
      } else {
        retFuture.completeExceptionally(future.cause());
      }
    });
    log.debug("Established a new connection to {}", ep);
    return retFuture;
  }

  /**
   * Channel initializer for TLS servers.
   */
  private class SslServerCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ChannelHandler dispatcher = new InboundMessageDispatcher();
    private final ChannelHandler encoder = new MessageEncoder(preamble);

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      KeyStore ts = KeyStore.getInstance("JKS");
      ts.load(new FileInputStream(tsLocation), tsPwd);
      tmFactory.init(ts);

      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(new FileInputStream(ksLocation), ksPwd);
      kmf.init(ks, ksPwd);

      SSLContext serverContext = SSLContext.getInstance("TLS");
      serverContext.init(kmf.getKeyManagers(), tmFactory.getTrustManagers(), null);

      SSLEngine serverSslEngine = serverContext.createSSLEngine();

      serverSslEngine.setNeedClientAuth(true);
      serverSslEngine.setUseClientMode(false);
      serverSslEngine.setEnabledProtocols(serverSslEngine.getSupportedProtocols());
      serverSslEngine.setEnabledCipherSuites(serverSslEngine.getSupportedCipherSuites());
      serverSslEngine.setEnableSessionCreation(true);

      channel.pipeline().addLast("ssl", new io.netty.handler.ssl.SslHandler(serverSslEngine))
          .addLast("encoder", encoder)
          .addLast("decoder", new MessageDecoder())
          .addLast("handler", dispatcher);
    }
  }

  /**
   * Channel initializer for TLS clients.
   */
  private class SslClientCommunicationChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ChannelHandler dispatcher = new InboundMessageDispatcher();
    private final ChannelHandler encoder = new MessageEncoder(preamble);

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      KeyStore ts = KeyStore.getInstance("JKS");
      ts.load(new FileInputStream(tsLocation), tsPwd);
      tmFactory.init(ts);

      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(new FileInputStream(ksLocation), ksPwd);
      kmf.init(ks, ksPwd);

      SSLContext clientContext = SSLContext.getInstance("TLS");
      clientContext.init(kmf.getKeyManagers(), tmFactory.getTrustManagers(), null);

      SSLEngine clientSslEngine = clientContext.createSSLEngine();

      clientSslEngine.setUseClientMode(true);
      clientSslEngine.setEnabledProtocols(clientSslEngine.getSupportedProtocols());
      clientSslEngine.setEnabledCipherSuites(clientSslEngine.getSupportedCipherSuites());
      clientSslEngine.setEnableSessionCreation(true);

      channel.pipeline().addLast("ssl", new io.netty.handler.ssl.SslHandler(clientSslEngine))
          .addLast("encoder", encoder)
          .addLast("decoder", new MessageDecoder())
          .addLast("handler", dispatcher);
    }
  }

  /**
   * Channel initializer for basic connections.
   */
  private class BasicChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ChannelHandler dispatcher = new InboundMessageDispatcher();
    private final ChannelHandler encoder = new MessageEncoder(preamble);

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      channel.pipeline()
          .addLast("encoder", encoder)
          .addLast("decoder", new MessageDecoder())
          .addLast("handler", dispatcher);
    }
  }

  /**
   * Channel inbound handler that dispatches messages to the appropriate handler.
   */
  @ChannelHandler.Sharable
  private class InboundMessageDispatcher extends SimpleChannelInboundHandler<Object> {
    // Effectively SimpleChannelInboundHandler<InternalMessage>,
    // had to specify <Object> to avoid Class Loader not being able to find some classes.

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object rawMessage) throws Exception {
      InternalMessage message = (InternalMessage) rawMessage;
      try {
        if (message.isRequest()) {
          RemoteServerConnection connection =
              serverConnections.computeIfAbsent(ctx.channel(), RemoteServerConnection::new);
          connection.dispatch(message);
        } else {
          RemoteClientConnection connection =
              clientConnections.computeIfAbsent(ctx.channel(), RemoteClientConnection::new);
          connection.dispatch(message);
        }
      } catch (RejectedExecutionException e) {
        log.warn("Unable to dispatch message due to {}", e.getMessage());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      log.error("Exception inside channel handling pipeline.", cause);

      RemoteClientConnection clientConnection = clientConnections.remove(context.channel());
      if (clientConnection != null) {
        clientConnection.close();
      }

      RemoteServerConnection serverConnection = serverConnections.remove(context.channel());
      if (serverConnection != null) {
        serverConnection.close();
      }
      context.close();
    }

    /**
     * Returns true if the given message should be handled.
     *
     * @param msg inbound message
     * @return true if {@code msg} is {@link InternalMessage} instance.
     * @see SimpleChannelInboundHandler#acceptInboundMessage(Object)
     */
    @Override
    public final boolean acceptInboundMessage(Object msg) {
      return msg instanceof InternalMessage;
    }
  }

  /**
   * Wraps a {@link CompletableFuture} and tracks its type and creation time.
   */
  private final class Callback {
    private final String type;
    private final CompletableFuture<byte[]> future;
    private final long time = System.currentTimeMillis();

    Callback(String type, CompletableFuture<byte[]> future) {
      this.type = type;
      this.future = future;
    }

    public void complete(byte[] value) {
      future.complete(value);
    }

    public void completeExceptionally(Throwable error) {
      future.completeExceptionally(error);
    }
  }

  /**
   * Represents the client side of a connection to a local or remote server.
   */
  private interface ClientConnection {

    /**
     * Sends a message to the other side of the connection.
     *
     * @param message the message to send
     * @return a completable future to be completed once the message has been sent
     */
    CompletableFuture<Void> sendAsync(InternalMessage message);

    /**
     * Sends a message to the other side of the connection, awaiting a reply.
     *
     * @param message the message to send
     * @return a completable future to be completed once a reply is received or the request times out
     */
    CompletableFuture<byte[]> sendAndReceive(InternalMessage message);

    /**
     * Closes the connection.
     */
    default void close() {
    }
  }

  /**
   * Represents the server side of a connection.
   */
  private interface ServerConnection {

    /**
     * Sends a reply to the other side of the connection.
     *
     * @param message the message to which to reply
     * @param status  the reply status
     * @param payload the response payload
     */
    void reply(InternalMessage message, InternalMessage.Status status, Optional<byte[]> payload);

    /**
     * Closes the connection.
     */
    default void close() {
    }
  }

  /**
   * Local connection implementation.
   */
  private final class LocalClientConnection implements ClientConnection {
    @Override
    public CompletableFuture<Void> sendAsync(InternalMessage message) {
      BiConsumer<InternalMessage, ServerConnection> handler = handlers.get(message.type());
      if (handler != null) {
        handler.accept(message, localServerConnection);
      } else {
        log.debug("No handler for message type {} from {}", message.type(), message.sender());
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(InternalMessage message) {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      BiConsumer<InternalMessage, ServerConnection> handler = handlers.get(message.type());
      if (handler != null) {
        handler.accept(message, new LocalServerConnection(future));
      } else {
        log.debug("No handler for message type {} from {}", message.type(), message.sender());
        new LocalServerConnection(future).reply(message, InternalMessage.Status.ERROR_NO_HANDLER, Optional.empty());
      }
      return future;
    }
  }

  /**
   * Local server connection.
   */
  private final class LocalServerConnection implements ServerConnection {
    private final CompletableFuture<byte[]> future;

    LocalServerConnection(CompletableFuture<byte[]> future) {
      this.future = future;
    }

    @Override
    public void reply(InternalMessage message, InternalMessage.Status status, Optional<byte[]> payload) {
      if (future != null) {
        if (status == InternalMessage.Status.OK) {
          future.complete(payload.orElse(EMPTY_PAYLOAD));
        } else if (status == InternalMessage.Status.ERROR_NO_HANDLER) {
          future.completeExceptionally(new MessagingException.NoRemoteHandler());
        } else if (status == InternalMessage.Status.ERROR_HANDLER_EXCEPTION) {
          future.completeExceptionally(new MessagingException.RemoteHandlerFailure());
        } else if (status == InternalMessage.Status.PROTOCOL_EXCEPTION) {
          future.completeExceptionally(new MessagingException.ProtocolException());
        }
      }
    }
  }

  /**
   * Remote connection implementation.
   */
  private final class RemoteClientConnection implements ClientConnection {
    private final Channel channel;
    private final Map<Long, Callback> futures = Maps.newConcurrentMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Cache<String, TimeoutHistory> timeoutHistories = CacheBuilder.newBuilder()
        .expireAfterAccess(HISTORY_EXPIRE_MILLIS, TimeUnit.MILLISECONDS)
        .build();

    RemoteClientConnection(Channel channel) {
      this.channel = channel;
    }

    /**
     * Times out callbacks for this connection.
     */
    private void timeoutCallbacks() {
      // Store the current time.
      long currentTime = System.currentTimeMillis();

      // Iterate through future callbacks and time out callbacks that have been alive
      // longer than the current timeout according to the message type.
      Iterator<Map.Entry<Long, Callback>> iterator = futures.entrySet().iterator();
      while (iterator.hasNext()) {
        Callback callback = iterator.next().getValue();
        try {
          TimeoutHistory timeoutHistory = timeoutHistories.get(callback.type, TimeoutHistory::new);
          long currentTimeout = timeoutHistory.currentTimeout;
          if (currentTime - callback.time > currentTimeout) {
            iterator.remove();
            long elapsedTime = currentTime - callback.time;
            timeoutHistory.addReplyTime(elapsedTime);
            callback.completeExceptionally(
                new TimeoutException("Request timed out in " + elapsedTime + " milliseconds"));
          }
        } catch (ExecutionException e) {
          throw new AssertionError();
        }
      }
    }

    @Override
    public CompletableFuture<Void> sendAsync(InternalMessage message) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      channel.writeAndFlush(message).addListener(channelFuture -> {
        if (!channelFuture.isSuccess()) {
          future.completeExceptionally(channelFuture.cause());
        } else {
          future.complete(null);
        }
      });
      return future;
    }

    @Override
    public CompletableFuture<byte[]> sendAndReceive(InternalMessage message) {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      Callback callback = new Callback(message.type(), future);
      futures.put(message.id(), callback);
      channel.writeAndFlush(message).addListener(channelFuture -> {
        if (!channelFuture.isSuccess()) {
          futures.remove(message.id());
          callback.completeExceptionally(channelFuture.cause());
        }
      });
      return future;
    }

    /**
     * Dispatches a message to a local handler.
     *
     * @param message the message to dispatch
     */
    private void dispatch(InternalMessage message) {
      if (message.preamble() != preamble) {
        log.debug("Received {} with invalid preamble from {}", message.type(), message.sender());
        return;
      }

      Callback callback = futures.remove(message.id());
      if (callback != null) {
        if (message.status() == InternalMessage.Status.OK) {
          callback.complete(message.payload());
        } else if (message.status() == InternalMessage.Status.ERROR_NO_HANDLER) {
          callback.completeExceptionally(new MessagingException.NoRemoteHandler());
        } else if (message.status() == InternalMessage.Status.ERROR_HANDLER_EXCEPTION) {
          callback.completeExceptionally(new MessagingException.RemoteHandlerFailure());
        } else if (message.status() == InternalMessage.Status.PROTOCOL_EXCEPTION) {
          callback.completeExceptionally(new MessagingException.ProtocolException());
        }

        try {
          TimeoutHistory timeoutHistory = timeoutHistories.get(callback.type, TimeoutHistory::new);
          timeoutHistory.addReplyTime(System.currentTimeMillis() - callback.time);
        } catch (ExecutionException e) {
          throw new AssertionError();
        }
      } else {
        log.debug("Received a reply for message id:[{}]. "
            + " from {}. But was unable to locate the"
            + " request handle", message.id(), message.sender());
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        timeoutFuture.cancel(false);
        for (Callback callback : futures.values()) {
          callback.completeExceptionally(new ConnectException());
        }
      }
    }
  }

  /**
   * Remote server connection.
   */
  private final class RemoteServerConnection implements ServerConnection {
    private final Channel channel;

    RemoteServerConnection(Channel channel) {
      this.channel = channel;
    }

    /**
     * Dispatches a message to a local handler.
     *
     * @param message the message to dispatch
     */
    private void dispatch(InternalMessage message) {
      if (message.preamble() != preamble) {
        log.debug("Received {} with invalid preamble from {}", message.type(), message.sender());
        reply(message, InternalMessage.Status.PROTOCOL_EXCEPTION, Optional.empty());
        return;
      }

      BiConsumer<InternalMessage, ServerConnection> handler = handlers.get(message.type());
      if (handler != null) {
        handler.accept(message, this);
      } else {
        log.debug("No handler for message type {} from {}", message.type(), message.sender());
        reply(message, InternalMessage.Status.ERROR_NO_HANDLER, Optional.empty());
      }
    }

    @Override
    public void reply(InternalMessage message, InternalMessage.Status status, Optional<byte[]> payload) {
      InternalMessage response = new InternalMessage(preamble,
          message.id(),
          localEndpoint,
          payload.orElse(EMPTY_PAYLOAD),
          status);
      channel.writeAndFlush(response);
    }
  }

  /**
   * Request-reply timeout history tracker.
   */
  private static final class TimeoutHistory {
    private final DescriptiveStatistics timeoutHistory = new SynchronizedDescriptiveStatistics(WINDOW_SIZE);
    private final AtomicLong maxReplyTime = new AtomicLong();
    private volatile long currentTimeout = DEFAULT_TIMEOUT_MILLIS;

    /**
     * Adds a reply time to the history.
     *
     * @param replyTime the reply time to add to the history
     */
    void addReplyTime(long replyTime) {
      maxReplyTime.getAndAccumulate(replyTime, Math::max);
    }

    /**
     * Computes the current timeout.
     */
    private void recomputeTimeoutMillis() {
      double nextTimeout = maxReplyTime.getAndSet(0) * TIMEOUT_MULTIPLIER;
      timeoutHistory.addValue(
          Math.min(Math.max(nextTimeout, MIN_TIMEOUT_MILLIS), MAX_TIMEOUT_MILLIS));
      if (timeoutHistory.getN() == WINDOW_SIZE) {
        this.currentTimeout = (long) timeoutHistory.getMax();
      }
    }
  }
}
