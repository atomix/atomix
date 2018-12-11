/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.bench;

import com.google.common.collect.Lists;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.NodeConfig;
import io.atomix.cluster.discovery.BootstrapDiscoveryConfig;
import io.atomix.cluster.discovery.MulticastDiscoveryConfig;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixConfig;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.net.Address;
import io.atomix.utils.net.MalformedAddressException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Atomix benchmark runner.
 */
public class AtomixBenchmark {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixBenchmark.class);

  /**
   * Runs a benchmark from the given command line arguments.
   *
   * @param args the program arguments
   * @throws Exception if the supplied arguments are invalid
   */
  public static void main(String[] args) throws Exception {
    // Parse the command line arguments.
    final List<String> unknown = new ArrayList<>();
    final Namespace namespace = parseArgs(args, unknown);
    final Namespace extraArgs = parseUnknown(unknown);
    extraArgs.getAttrs().forEach((key, value) -> System.setProperty(key, value.toString()));

    Atomix atomix = buildAtomix(namespace);
    atomix.start().join();
    LOGGER.info("Atomix listening at {}", atomix.getMembershipService().getLocalMember().address());

    final ManagedRestService rest = buildRestService(atomix, namespace);
    rest.start().join();
    LOGGER.warn("The Atomix HTTP API is BETA and is intended for development and debugging purposes only!");
    LOGGER.info("HTTP server listening at {}", rest.address());

    new AtomixBenchmark(buildAtomix(namespace)).start();

    synchronized (Atomix.class) {
      while (atomix.isRunning()) {
        Atomix.class.wait();
      }
    }
  }

  private final Atomix atomix;
  private final Map<String, BenchmarkController> controllers = new ConcurrentHashMap<>();
  private final Map<String, BenchmarkExecutor> executors = new ConcurrentHashMap<>();

  AtomixBenchmark(Atomix atomix) {
    this.atomix = atomix;
  }

  /**
   * Starts the instance.
   */
  void start() {
    atomix.getCommunicationService().<BenchmarkConfig, String>subscribe(
        BenchmarkConstants.START_SUBJECT, BenchmarkSerializer.INSTANCE::decode, this::startBenchmark, BenchmarkSerializer.INSTANCE::encode);
    atomix.getCommunicationService().<BenchmarkConfig, Void>subscribe(
        BenchmarkConstants.RUN_SUBJECT, BenchmarkSerializer.INSTANCE::decode, this::runBenchmark, BenchmarkSerializer.INSTANCE::encode);
    atomix.getCommunicationService().<String, Void>subscribe(
        BenchmarkConstants.KILL_SUBJECT, BenchmarkSerializer.INSTANCE::decode, this::killBenchmark, BenchmarkSerializer.INSTANCE::encode);
    atomix.getCommunicationService().<String, BenchmarkProgress>subscribe(
        BenchmarkConstants.PROGRESS_SUBJECT, BenchmarkSerializer.INSTANCE::decode, this::getProgress, BenchmarkSerializer.INSTANCE::encode);
    atomix.getCommunicationService().<String, BenchmarkResult>subscribe(
        BenchmarkConstants.RESULT_SUBJECT, BenchmarkSerializer.INSTANCE::decode, this::getResult, BenchmarkSerializer.INSTANCE::encode);
    atomix.getCommunicationService().<String, Void>subscribe(
        BenchmarkConstants.STOP_SUBJECT, BenchmarkSerializer.INSTANCE::decode, this::stopBenchmark, BenchmarkSerializer.INSTANCE::encode);
  }

  private CompletableFuture<String> startBenchmark(BenchmarkConfig config) {
    BenchmarkController controller = controllers.get(config.getBenchId());
    if (controller == null) {
      controller = new BenchmarkController(atomix, config);
      controllers.put(controller.getBenchId(), controller);
      return controller.start().thenApply(v -> config.getBenchId());
    }
    return CompletableFuture.completedFuture(null);
  }

  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> runBenchmark(BenchmarkConfig config) {
    BenchmarkExecutor executor = executors.get(config.getBenchId());
    if (executor == null) {
      executor = BenchmarkType.forTypeName(config.getType()).createExecutor(atomix);
      executors.put(config.getBenchId(), executor);
      executor.start(config);
    }
    return CompletableFuture.completedFuture(null);
  }

  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> killBenchmark(String benchId) {
    BenchmarkExecutor executor = executors.get(benchId);
    if (executor != null) {
      executor.stop();
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<BenchmarkProgress> getProgress(String benchId) {
    BenchmarkController controller = controllers.get(benchId);
    if (controller != null) {
      return CompletableFuture.completedFuture(controller.getProgress());
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<BenchmarkResult> getResult(String benchId) {
    BenchmarkController controller = controllers.get(benchId);
    if (controller != null) {
      return CompletableFuture.completedFuture(controller.getResult());
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<Void> stopBenchmark(String benchId) {
    BenchmarkController controller = controllers.get(benchId);
    if (controller != null) {
      return controller.stop().thenRun(() -> controllers.remove(benchId));
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Parses the command line arguments, returning an argparse4j namespace.
   *
   * @param args the arguments to parse
   * @return the namespace
   */
  static Namespace parseArgs(String[] args, List<String> unknown) {
    ArgumentParser parser = createParser();
    Namespace namespace = null;
    try {
      namespace = parser.parseKnownArgs(args, unknown);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }
    return namespace;
  }

  /**
   * Parses unknown arguments, returning an argparse4j namespace.
   *
   * @param unknown the unknown arguments to parse
   * @return the namespace
   * --foo.bar baz --bar.baz foo bar --foo.bar.baz bang
   */
  static Namespace parseUnknown(List<String> unknown) {
    Map<String, Object> attrs = new HashMap<>();
    String attr = null;
    for (String arg : unknown) {
      if (arg.startsWith("--")) {
        int splitIndex = arg.indexOf('=');
        if (splitIndex == -1) {
          attr = arg.substring(2);
        } else {
          attrs.put(arg.substring(2, splitIndex), arg.substring(splitIndex + 1));
          attr = null;
        }
      } else if (attr != null) {
        attrs.put(attr, arg);
        attr = null;
      }
    }
    return new Namespace(attrs);
  }

  /**
   * Creates an agent argument parser.
   */
  private static ArgumentParser createParser() {
    // Argument type for node ID/location formatted id@host:port.
    final ArgumentType<NodeConfig> nodeArgumentType = (ArgumentParser argumentParser, Argument argument, String value) -> new NodeConfig()
        .setId(parseMemberId(value))
        .setAddress(parseAddress(value));

    // Argument type for node addresses formatted host:port.
    final ArgumentType<Address> addressArgumentType = (argumentParser, argument, value) -> Address.from(value);

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Runs the Atomix agent with the given arguments. Arbitrary configuration options may be overridden " +
            "by specifying the option path and value as an optional argument, e.g. --cluster.node.id node-1");
    parser.addArgument("--member", "-m")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The local member identifier, used in intra-cluster communication.");
    parser.addArgument("--address", "-a")
        .type(addressArgumentType)
        .metavar("HOST:PORT")
        .nargs("?")
        .required(false)
        .help("The address for the local member. If no address is specified, the first public interface will be used.");
    parser.addArgument("--config", "-c")
        .metavar("CONF|JSON|PROPERTIES")
        .type(File.class)
        .nargs("*")
        .required(false)
        .setDefault(System.getProperty("atomix.config.files") != null
            ? Lists.newArrayList(System.getProperty("atomix.config.files").split(","))
            : Lists.newArrayList())
        .help("The Atomix configuration. Can be specified as a file path or JSON/YAML string.");
    parser.addArgument("--data-dir")
        .metavar("FILE")
        .type(String.class)
        .nargs("?")
        .setDefault(System.getProperty("atomix.data", ".data"))
        .help("The default Atomix data directory. Defaults to .data");
    parser.addArgument("--bootstrap", "-b")
        .nargs("*")
        .type(nodeArgumentType)
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The set of static members to join. When provided, bootstrap node discovery will be used.");
    parser.addArgument("--multicast")
        .action(new StoreTrueArgumentAction())
        .setDefault(false)
        .help("Enables multicast discovery. Note that the network must support multicast for this feature to work.");
    parser.addArgument("--multicast-group")
        .type(String.class)
        .metavar("IP")
        .help("Sets the multicast group. Defaults to 230.0.0.1");
    parser.addArgument("--multicast-port")
        .type(Integer.class)
        .metavar("PORT")
        .help("Sets the multicast port. Defaults to 54321");
    parser.addArgument("--http-host")
        .type(String.class)
        .metavar("HOST")
        .required(false)
        .setDefault("0.0.0.0")
        .help("Sets the host to which to bind the HTTP server. Defaults to 0.0.0.0 (all interfaces)");
    parser.addArgument("--http-port", "-p")
        .type(Integer.class)
        .metavar("PORT")
        .required(false)
        .setDefault(5678)
        .help("Sets the port on which to run the HTTP server. Defaults to 5678");
    return parser;
  }

  /**
   * Creates an Atomix configuration from the given namespace.
   *
   * @param namespace the namespace from which to create the configuration
   * @return the Atomix configuration for the given namespace
   */
  static AtomixConfig createConfig(Namespace namespace) {
    final List<File> configFiles = namespace.getList("config");
    final String memberId = namespace.getString("member");
    final Address address = namespace.get("address");
    final List<NodeConfig> bootstrap = namespace.getList("bootstrap");
    final boolean multicastEnabled = namespace.getBoolean("multicast");
    final String multicastGroup = namespace.get("multicast_group");
    final Integer multicastPort = namespace.get("multicast_port");

    System.setProperty("atomix.data", namespace.getString("data_dir"));

    // If a configuration was provided, merge the configuration's member information with the provided command line arguments.
    AtomixConfig config;
    if (configFiles != null && !configFiles.isEmpty()) {
      System.setProperty("atomix.config.resources", "");
      config = Atomix.config(configFiles);
    } else {
      config = Atomix.config();
    }

    if (memberId != null) {
      config.getClusterConfig().getNodeConfig().setId(memberId);
    }

    if (address != null) {
      config.getClusterConfig().getNodeConfig().setAddress(address);
    }

    if (bootstrap != null && !bootstrap.isEmpty()) {
      config.getClusterConfig().setDiscoveryConfig(new BootstrapDiscoveryConfig().setNodes(bootstrap));
    }

    if (multicastEnabled) {
      config.getClusterConfig().getMulticastConfig().setEnabled(true);
      if (multicastGroup != null) {
        config.getClusterConfig().getMulticastConfig().setGroup(multicastGroup);
      }
      if (multicastPort != null) {
        config.getClusterConfig().getMulticastConfig().setPort(multicastPort);
      }
      if (bootstrap == null || bootstrap.isEmpty()) {
        config.getClusterConfig().setDiscoveryConfig(new MulticastDiscoveryConfig());
      }
    }
    return config;
  }

  /**
   * Builds a new Atomix instance from the given namespace.
   *
   * @param namespace the namespace from which to build the instance
   * @return the Atomix instance
   */
  private static Atomix buildAtomix(Namespace namespace) {
    return Atomix.builder(createConfig(namespace))
        .withProperty(BenchmarkConstants.BENCH_NODE_TYPE, Boolean.TRUE.toString())
        .withShutdownHookEnabled()
        .build();
  }

  /**
   * Builds a REST service for the given Atomix instance from the given namespace.
   *
   * @param atomix the Atomix instance
   * @param namespace the namespace from which to build the service
   * @return the managed REST service
   */
  private static ManagedRestService buildRestService(Atomix atomix, Namespace namespace) {
    final String httpHost = namespace.getString("http_host");
    final Integer httpPort = namespace.getInt("http_port");
    return RestService.builder()
        .withAtomix(atomix)
        .withAddress(Address.from(httpHost, httpPort))
        .build();
  }

  static MemberId parseMemberId(String address) {
    int endIndex = address.indexOf('@');
    if (endIndex > 0) {
      return MemberId.from(address.substring(0, endIndex));
    }
    return null;
  }

  static Address parseAddress(String address) {
    int startIndex = address.indexOf('@');
    if (startIndex == -1) {
      try {
        return Address.from(address);
      } catch (MalformedAddressException e) {
        return Address.local();
      }
    } else {
      return Address.from(address.substring(startIndex + 1));
    }
  }
}
