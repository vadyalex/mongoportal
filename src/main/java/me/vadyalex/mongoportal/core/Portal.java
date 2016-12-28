package me.vadyalex.mongoportal.core;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import me.vadyalex.rill.Rill;
import org.bson.Document;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Portal {

    public static final IndexOptions CREATE_INDEX_OPTIONS = new IndexOptions().background(true);
    public static final RenameCollectionOptions RENAME_COLLECTION_OPTIONS = new RenameCollectionOptions().dropTarget(true);

    public static class Builder {

        private MongoClient fromMongoClient;
        private MongoClient toMongoClient;

        private String fromDatabaseName;
        private String toDatabaseName;

        private String fromCollectionName;
        private String toCollectionName;

        private boolean quiet = false;

        private void checkArgument(String s) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(s));
        }

        public Builder from(MongoClient mongoClient) {
            Preconditions.checkNotNull(mongoClient);
            this.fromMongoClient = mongoClient;
            return this;
        }

        public Builder to(MongoClient mongoClient) {
            Preconditions.checkNotNull(mongoClient);
            this.toMongoClient = mongoClient;
            return this;
        }

        public Builder database(String databaseName) {
            checkArgument(databaseName);
            this.fromDatabaseName = databaseName;
            return this;
        }

        public Builder collection(String collectionName) {
            checkArgument(collectionName);
            this.fromCollectionName = collectionName;
            return this;
        }

        public Builder toDatabase(String databaseName) {
            this.toDatabaseName = databaseName;
            return this;
        }

        public Builder toCollection(String collectionName) {
            this.toCollectionName = collectionName;
            return this;
        }

        public Builder quiet() {
            this.quiet = true;
            return this;
        }

        public Portal build() {
            return new Portal(
                    fromMongoClient,
                    toMongoClient != null ? toMongoClient : fromMongoClient,
                    fromDatabaseName,
                    toDatabaseName != null ? toDatabaseName : fromDatabaseName,
                    fromCollectionName,
                    toCollectionName != null ? toCollectionName : fromCollectionName,
                    quiet
            );
        }

    }

    public static final Builder builder() {
        return new Builder();
    }

    private static final String TMP_COLLECTION_PREFIX = "mngtlprt";

    private static final ForkJoinPool pool = new ForkJoinPool();

    private final MongoClient fromMongoClient;
    private final MongoClient toMongoClient;

    private final String fromDatabaseName;
    private final String toDatabaseName;

    private final String fromCollectionName;
    private final String toCollectionName;

    private boolean quiet;

    protected Portal(MongoClient fromMongoClient, MongoClient toMongoClient, String fromDatabaseName, String toDatabaseName, String fromCollectionName, String toCollectionName, boolean quiet) {
        Preconditions.checkNotNull(fromMongoClient);
        Preconditions.checkNotNull(toMongoClient);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fromDatabaseName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(toDatabaseName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fromCollectionName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(toCollectionName));

        this.fromMongoClient = fromMongoClient;
        this.toMongoClient = toMongoClient;

        this.fromDatabaseName = fromDatabaseName;
        this.toDatabaseName = toDatabaseName;

        this.fromCollectionName = fromCollectionName;
        this.toCollectionName = toCollectionName;

        this.quiet = quiet;
    }

    protected String generateTmpCollectionName() {
        return TMP_COLLECTION_PREFIX + UUID.randomUUID().toString().replaceAll("-", "");
    }

    public boolean teleport() {

        if (fromMongoClient.getDatabaseNames().stream().noneMatch(databaseName -> databaseName.equals(fromDatabaseName))) {
            System.out.println(
                    String.format("\tNothing to teleport. Database '%s' does not exist", fromDatabaseName)
            );
            return false;
        }

        final MongoDatabase fromDB = fromMongoClient.getDatabase(fromDatabaseName);

        if (Rill.from(fromDB.listCollectionNames()).noneMatch(collectionName -> collectionName.equals(fromCollectionName))) {
            System.out.println(
                    String.format("\tNothing to teleport. Collection '%s' does not exist in database '%s'", fromCollectionName, fromDatabaseName)
            );
            return false;
        }

        final MongoCollection<Document> fromDBCollection = fromDB.getCollection(
                fromCollectionName
        );

        final long count = fromDBCollection.count();
        if (count == 0) {
            System.out.println(
                    String.format("\tNothing to teleport. Collection '%s' is empty", fromDBCollection.getNamespace().getFullName())
            );
            return false;
        } else {
            System.out.println(
                    String.format("\tThere are %s documents in collection '%s' to teleport", count, fromDBCollection.getNamespace().getFullName())
            );
        }

        final MongoDatabase toDB = toMongoClient.getDatabase(
                toDatabaseName
        );

        final String tmpCollectionName = generateTmpCollectionName();

        final MongoCollection tmpDBCollection = toDB.getCollection(
                tmpCollectionName
        );

        System.out.println(
                String.format("\tTeleporting data to '%s'..", tmpDBCollection.getNamespace().getFullName())
        );

        final Progress progress = Progress.start(count);
        final ForkJoinTask<Void> task = pool.submit(
                new TeleportationAction(
                        fromDBCollection,
                        tmpDBCollection,
                        0,
                        count,
                        quiet ?
                                Optional.<Progress>empty()
                                :
                                Optional.of(progress)
                )
        );

        System.out.println();

        try {
            // TODO must be configurable
            task.get(1, TimeUnit.HOURS);
        } catch (Exception e) {
            System.out.println("\tData teleportation failed..");

            e.printStackTrace();

            dropCollection(tmpDBCollection);

            return false;
        }

        if (task.isCompletedNormally()) {
            System.out.println(
                    progress.status()
            );

            System.out.println(
                    String.format("\tTemporary collection '%s' now contains %s documents", tmpCollectionName, tmpDBCollection.count())
            );


            final List<Document> keys = Rill
                    .from(
                            fromDBCollection
                                    .listIndexes()
                                    // TODO must be configurable
                                    .maxTime(10, TimeUnit.SECONDS)
                    )
                    .filter(
                            document -> !"_id_".equals(document.getString("name"))
                    )
                    .filter(
                            document -> document.containsKey("key")
                    )
                    .map(
                            document -> (Document) document.get("key")
                    )
                    .collect(
                            Collectors.toList()
                    );

            if (!keys.isEmpty()) {

                System.out.println("\tCreate indexes..");

                try {

                    keys.forEach(
                            key -> {
                                System.out.println(
                                        String.format("\tCreate index '%s' in background", key)
                                );

                                toDB
                                        .getCollection(
                                                toCollectionName
                                        )
                                        .createIndex(
                                                key,
                                                CREATE_INDEX_OPTIONS
                                        );

                                System.out.println(
                                        "\t\tComplete!"
                                );
                            }
                    );

                } catch (Exception e) {
                    System.out.println("\tIndex creation failed! Terminating teleportation..");

                    e.printStackTrace();

                    dropCollection(tmpDBCollection);

                    return false;
                }
            }

            System.out.println();

            if (Rill.from(toDB.listCollectionNames()).anyMatch(collectionName -> collectionName.equals(toCollectionName))) {
                System.out.println(String.format("\tCollection '%s' is already exists in '%s' database. Overwrite.. ", toCollectionName, toDatabaseName));
            }

            System.out.println(String.format("\tRenaming temporary collection '%s' to '%s'", tmpCollectionName, toCollectionName));

            tmpDBCollection.renameCollection(
                    new MongoNamespace(toDatabaseName, toCollectionName),
                    RENAME_COLLECTION_OPTIONS
            );
        }

        System.out.println("\tDone.");

        return true;

    }

    protected void dropCollection(MongoCollection tmpDBCollection) {
        System.out.println("\tRemoving temporary collection..");
        tmpDBCollection.drop();
        System.out.println("\tDone.");
    }

}
