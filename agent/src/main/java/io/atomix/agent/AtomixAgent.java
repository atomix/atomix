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

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixConfig;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.config.Configs;
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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Atomix agent runner.
 */
public class AtomixAgent {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixAgent.class);

  public static void main(String[] args) throws Exception {
    Function<Member.Type, ArgumentType<MemberConfig>> memberArgumentType = (type) -> (ArgumentParser argumentParser, Argument argument, String value) -> {
      return new MemberConfig()
          .setId(parseMemberId(value))
          .setType(type)
          .setAddress(parseAddress(value));
    };

    ArgumentType<Member.Type> typeArgumentType = (argumentParser, argument, value) -> Member.Type.valueOf(value.toUpperCase());
    ArgumentType<Address> addressArgumentType = (argumentParser, argument, value) -> Address.from(value);

    ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Atomix server");
    parser.addArgument("member")
        .type(memberArgumentType.apply(Member.Type.PERSISTENT))
        .nargs("?")
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The member info for the local member. This should be in the format [NAME@]HOST[:PORT]. " +
            "If no name is provided, the member name will default to the host. " +
            "If no port is provided, the port will default to 5679.");
    parser.addArgument("--type", "-t")
        .type(typeArgumentType)
        .metavar("TYPE")
        .choices(Member.Type.PERSISTENT, Member.Type.EPHEMERAL)
        .setDefault(Member.Type.PERSISTENT)
        .help("Indicates the local member type.");
    parser.addArgument("--config", "-c")
        .metavar("FILE|JSON|YAML")
        .required(false)
        .help("The Atomix configuration. Can be specified as a file path or JSON/YAML string.");
    parser.addArgument("--persistent-members", "-n")
        .nargs("*")
        .type(memberArgumentType.apply(Member.Type.PERSISTENT))
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The set of core members, if any. When bootstrapping a new cluster, if the local member is a core member " +
            "then it should be present in the core configuration as well.");
    parser.addArgument("--ephemeral-members", "-b")
        .nargs("*")
        .type(memberArgumentType.apply(Member.Type.EPHEMERAL))
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The set of bootstrap members. If core members are provided, the cluster will be bootstrapped from the " +
            "core members. For clusters without core members, at least one bootstrap member must be provided unless " +
            "using multicast discovery or bootstrapping a new cluster.");
    parser.addArgument("--multicast", "-m")
        .action(new StoreTrueArgumentAction())
        .setDefault(false)
        .help("Enables multicast discovery. Note that the network must support multicast for this feature to work.");
    parser.addArgument("--multicast-address", "-a")
        .type(addressArgumentType)
        .metavar("HOST:PORT")
        .help("Sets the multicast discovery address. Defaults to 230.0.0.1:54321");
    parser.addArgument("--http-port", "-p")
        .type(Integer.class)
        .metavar("PORT")
        .required(false)
        .setDefault(5678)
        .help("Sets the port on which to run the HTTP server. Defaults to 5678");

    Namespace namespace = null;
    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    final String configString = namespace.get("config");
    final MemberConfig localMember = namespace.get("member");
    final Member.Type localMemberType = namespace.get("type");
    final List<MemberConfig> persistentMembers = namespace.getList("persistent_members");
    final List<MemberConfig> ephemeralMembers = namespace.getList("ephemeral_members");
    final boolean multicastEnabled = namespace.getBoolean("multicast");
    final Address multicastAddress = namespace.get("multicast_address");
    final Integer httpPort = namespace.getInt("http_port");

    // If a configuration was provided, merge the configuration's member information with the provided command line arguments.
    final Atomix.Builder builder;
    if (configString != null) {
      AtomixConfig config = loadConfig(configString);
      if (localMember != null) {
        config.getClusterConfig().getMembers().values().stream()
            .filter(member -> member.getId().equals(localMember.getId()))
            .findAny()
            .ifPresent(localMemberConfig -> {
              if (localMemberType == null) {
                localMember.setType(localMemberConfig.getType());
              }
              localMember.setAddress(localMemberConfig.getAddress());
              localMember.setZone(localMemberConfig.getZone());
              localMember.setRack(localMemberConfig.getRack());
              localMember.setHost(localMemberConfig.getHost());
            });
      }
      builder = Atomix.builder(config);
    } else {
      builder = Atomix.builder();
    }

    builder.withShutdownHook(true);

    // If a local member was provided, add the local member to the builder.
    if (localMember != null) {
      localMember.setType(localMemberType);
      Member member = new Member(localMember);
      builder.withLocalMember(member);
      LOGGER.info("member: {}", member);
    }

    // If a cluster configuration was provided, add all the cluster members to the builder.
    if (persistentMembers != null || ephemeralMembers != null) {
      List<Member> members = Stream.concat(
          persistentMembers != null ? persistentMembers.stream() : Stream.empty(),
          ephemeralMembers != null ? ephemeralMembers.stream() : Stream.empty())
          .map(member -> Member.builder(member.getId())
              .withType(member.getType())
              .withAddress(member.getAddress())
              .build())
          .collect(Collectors.toList());
      builder.withMembers(members);
    }

    // Enable multicast if provided.
    if (multicastEnabled) {
      builder.withMulticastEnabled();
      if (multicastAddress != null) {
        builder.withMulticastAddress(multicastAddress);
      }
    }

    Atomix atomix = builder.build();

    atomix.start().join();

    LOGGER.info("Atomix listening at {}:{}", atomix.membershipService().getLocalMember().address().host(), atomix.membershipService().getLocalMember().address().port());

    ManagedRestService rest = RestService.builder()
        .withAtomix(atomix)
        .withAddress(Address.from(atomix.membershipService().getLocalMember().address().host(), httpPort))
        .build();

    rest.start().join();

    LOGGER.info("HTTP server listening at {}:{}", atomix.membershipService().getLocalMember().address().address().getHostAddress(), httpPort);

    synchronized (Atomix.class) {
      while (atomix.isRunning()) {
        Atomix.class.wait();
      }
    }
  }

  /**
   * Loads a configuration from the given file.
   */
  private static AtomixConfig loadConfig(String config) {
    File configFile = new File(config);
    if (configFile.exists()) {
      return Configs.load(configFile, AtomixConfig.class);
    } else {
      return Configs.load(config, AtomixConfig.class);
    }
  }

  static MemberId parseMemberId(String address) {
    int endIndex = address.indexOf('@');
    if (endIndex > 0) {
      return MemberId.from(address.substring(0, endIndex));
    } else {
      try {
        return MemberId.from(Address.from(address).host());
      } catch (MalformedAddressException e) {
        return MemberId.from(address);
      }
    }
  }

  static Address parseAddress(String address) {
    int startIndex = address.indexOf('@');
    if (startIndex == -1) {
      try {
        return Address.from(address);
      } catch (MalformedAddressException e) {
        return Address.from("0.0.0.0");
      }
    } else {
      return Address.from(address.substring(startIndex + 1));
    }
  }
}
