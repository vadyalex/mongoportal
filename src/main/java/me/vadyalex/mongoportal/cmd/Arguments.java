package me.vadyalex.mongoportal.cmd;


import com.beust.jcommander.Parameter;
import com.google.common.net.HostAndPort;
import com.mongodb.ServerAddress;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Arguments {

    @Parameter(names = {"-h", "--host"}, description = "Single instance or replica set hosts with or without port where data is located", required = true)
    private List<String> hosts;

    @Parameter(names = {"-th", "--to-host"}, description = "Single or replica set hosts with or without port where to teleport data to. May be omitted then original location is used")
    private List<String> toHosts;

    @Parameter(names = {"-d", "--db"}, description = "Database name where data is located", required = true)
    private String database;

    @Parameter(names = {"-td", "--to-db"}, description = "Database name where data to teleport data to. May be omitted then original database name is used")
    private String toDatabase;

    @Parameter(names = {"-c", "--collection"}, description = "Collection name where data is located", required = true)
    private String collection;

    @Parameter(names = {"-tc", "--to-collection"}, description = "Collection name where to teleport data to. May be omitted then original collection name is used")
    private String toCollection;

    @Parameter(names = {"--help"}, description = "Print usage info", help = true)
    private boolean help;

    public static final Function<String, ServerAddress> TO_SERVER_ADDRESS = host -> {

        final HostAndPort hostAndPort = HostAndPort.fromString(host).withDefaultPort(27017);

        return new ServerAddress(
                hostAndPort.getHostText(),
                hostAndPort.getPort()
        );
    };

    public List<String> getHosts() {
        return hosts;
    }

    public List<String> getToHosts() {
        return toHosts;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public String getToCollection() {
        return toCollection;
    }

    public String getToDatabase() {
        return toDatabase;
    }

    public boolean isHelp() {
        return help;
    }

    public List<ServerAddress> getHostsAsServerAddress() {
        return getHosts() == null ?
                null
                :
                getHosts()
                        .stream()
                        .map(TO_SERVER_ADDRESS)
                        .collect(
                                Collectors.toList()
                        );
    }

    public List<ServerAddress> getToHostsAsServerAddress() {
        return getToHosts() == null ?
                null
                :
                getToHosts()
                        .stream()
                        .map(TO_SERVER_ADDRESS)
                        .collect(
                                Collectors.toList()
                        );
    }
}
