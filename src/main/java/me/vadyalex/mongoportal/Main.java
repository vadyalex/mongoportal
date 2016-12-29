package me.vadyalex.mongoportal;


import com.beust.jcommander.JCommander;
import com.mongodb.MongoClient;
import me.vadyalex.mongoportal.cmd.Arguments;
import me.vadyalex.mongoportal.core.Portal;

public class Main {

    public static void main(String[] args) {

        final Arguments arguments = new Arguments();
        final JCommander commander = new JCommander();
        commander.setProgramName("mongoportal");
        commander.addObject(arguments);

        if (args.length == 0) {
            commander.usage();
            return;
        } else {
            try {
                commander.parse(args);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println("Run without arguments to see usage information");
                System.exit(1);
            }
        }

        if (arguments.isHelp()) {
            commander.usage();
            return;
        }

        if (arguments.isVersion()) {
            System.out.println(
                    arguments.getVersion()
            );
            return;
        }

        final MongoClient fromMongoClient = new MongoClient(
                arguments.getHostsAsServerAddress()
        );

        final MongoClient toMongoClient = arguments.getToHostsAsServerAddress() != null ?
                new MongoClient(
                        arguments.getToHostsAsServerAddress()
                )
                :
                fromMongoClient;

        final boolean result = Portal
                .builder()
                .from(
                        fromMongoClient
                )
                .to(
                        toMongoClient
                )
                .database(
                        arguments.getDatabase()
                )
                .collection(
                        arguments.getCollection()
                )
                .toDatabase(
                        arguments.getToDatabase()
                )
                .toCollection(
                        arguments.getToCollection()
                )
                .build()
                .teleport();

        fromMongoClient.close();
        toMongoClient.close();

        if (!result)
            System.exit(1);
    }


}
