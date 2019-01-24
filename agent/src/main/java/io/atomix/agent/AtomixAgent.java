/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.agent;

import ch.qos.logback.classic.Level;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Atomix agent runner.
 */
public class AtomixAgent {

  /**
   * Runs a standalone Atomix agent from the given command line arguments.
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

    final Logger logger = createLogger(namespace);

    final Atomix atomix = buildAtomix(namespace);
    atomix.start().join();
    logger.info("Atomix listening at {}", atomix.getMembershipService().getLocalMember().address());

    final ManagedRestService rest = buildRestService(atomix, namespace);
    rest.start().join();
    logger.warn("The Atomix HTTP API is BETA and is intended for development and debugging purposes only!");
    logger.info("HTTP server listening at {}", rest.address());

    synchronized (Atomix.class) {
      while (atomix.isRunning()) {
        Atomix.class.wait();
      }
    }
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

    // A list of all available logback log levels.
    final List<String> logLevels = Arrays.asList(
        Level.ALL.toString(),
        Level.OFF.toString(),
        Level.ERROR.toString(),
        Level.WARN.toString(),
        Level.INFO.toString(),
        Level.DEBUG.toString(),
        Level.TRACE.toString());

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Runs the Atomix agent with the given arguments. Arbitrary configuration options may be overridden "
            + "by specifying the option path and value as an optional argument, e.g. --cluster.node.id node-1");
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
    parser.addArgument("--host")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The host on which this member runs, used for host-aware partition management.");
    parser.addArgument("--rack")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The rack on which this member runs, used for rack-aware partition management.");
    parser.addArgument("--zone")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The zone in which this member runs, used for zone-aware partition management.");
    parser.addArgument("--config", "-c")
        .metavar("CONF|JSON|PROPERTIES")
        .type(File.class)
        .nargs("*")
        .required(false)
        .setDefault(System.getProperty("atomix.config.files") != null
            ? Lists.newArrayList(System.getProperty("atomix.config.files").split(","))
            : Lists.newArrayList())
        .help("The Atomix configuration. Can be specified as a file path or JSON/YAML string.");
    parser.addArgument("--ignore-resources")
        .action(new StoreTrueArgumentAction())
        .setDefault(false)
        .help("Ignores classpath resources when loading configuration files. Only valid when configuration file(s) are provided.");
    parser.addArgument("--log-config")
        .metavar("FILE")
        .type(String.class)
        .nargs("?")
        .setDefault(System.getProperty("atomix.logback"))
        .help("The path to an optional logback configuration file outside the classpath.");
    parser.addArgument("--log-dir")
        .metavar("FILE")
        .type(String.class)
        .nargs("?")
        .setDefault(System.getProperty("atomix.log.directory", new File(System.getProperty("user.dir"), "logs").getPath()))
        .help("The path to the Atomix log directory. "
            + "This option is only valid for logback configurations that employ the atomix.log.directory property.");
    parser.addArgument("--log-level")
        .metavar("LEVEL")
        .type(String.class)
        .choices(logLevels)
        .nargs("?")
        .setDefault(System.getProperty("atomix.log.level", Level.DEBUG.toString()))
        .help("The globally filtered log level for all Atomix logs. "
            + "This option is only valid for logback configurations that employ the atomix.log.level property.");
    parser.addArgument("--file-log-level")
        .metavar("LEVEL")
        .type(String.class)
        .choices(logLevels)
        .nargs("?")
        .setDefault(System.getProperty("atomix.log.file.level", Level.DEBUG.toString()))
        .help("The file log level. This option is only valid for logback configurations that employ the "
            + "atomix.log.file.level property.");
    parser.addArgument("--console-log-level")
        .metavar("LEVEL")
        .type(String.class)
        .choices(logLevels)
        .nargs("?")
        .setDefault(System.getProperty("atomix.log.console.level", Level.INFO.toString()))
        .help("The console log level. This option is only valid for logback configurations that employ the "
            + "atomix.log.console.level property.");
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
   * Configures and creates a new logger for the given namespace.
   *
   * @param namespace the namespace from which to create the logger configuration
   * @return a new agent logger
   */
  static Logger createLogger(Namespace namespace) {
    String logConfig = namespace.getString("log_config");
    if (logConfig != null) {
      System.setProperty("logback.configurationFile", logConfig);
    }
    System.setProperty("atomix.log.directory", namespace.getString("log_dir"));
    System.setProperty("atomix.log.level", namespace.getString("log_level"));
    System.setProperty("atomix.log.console.level", namespace.getString("console_log_level"));
    System.setProperty("atomix.log.file.level", namespace.getString("file_log_level"));
    return LoggerFactory.getLogger(AtomixAgent.class);
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
    final String host = namespace.getString("host");
    final String rack = namespace.getString("rack");
    final String zone = namespace.getString("zone");
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

    if (host != null) {
      config.getClusterConfig().getNodeConfig().setHostId(host);
    }
    if (rack != null) {
      config.getClusterConfig().getNodeConfig().setRackId(rack);
    }
    if (zone != null) {
      config.getClusterConfig().getNodeConfig().setZoneId(zone);
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
    return Atomix.builder(createConfig(namespace)).withShutdownHookEnabled().build();
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
