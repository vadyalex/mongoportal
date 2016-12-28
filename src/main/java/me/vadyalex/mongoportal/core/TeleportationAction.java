package me.vadyalex.mongoportal.core;

import com.google.common.base.Preconditions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

public class TeleportationAction extends RecursiveAction {

    // TODO Make it configurable as well
    public static final int BATCH_SIZE = 500;

    // TODO I think it should be configurable as well?
    public static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions()
            .ordered(false)
            .bypassDocumentValidation(true);

    private final MongoCollection<Document> from;
    private final MongoCollection<Document> to;

    private final long low;
    private final long high;
    private final long size;

    private final Optional<Progress> progress;

    public TeleportationAction(MongoCollection from, MongoCollection to, long low, long high) {
        this(from, to, low, high, Optional.empty());
    }

    public TeleportationAction(
            MongoCollection from,
            MongoCollection to,
            long low,
            long high,
            Optional<Progress> progress
    ) {
        Preconditions.checkNotNull(from);
        Preconditions.checkNotNull(to);
        Preconditions.checkNotNull(progress);
        Preconditions.checkArgument(low >= 0);
        Preconditions.checkArgument(high >= 0);
        Preconditions.checkArgument(low <= high, "Incorrect range {} <= {}", low, high);

        this.from = from;
        this.to = to;

        this.low = low;
        this.high = high;
        this.size = high - low;

        this.progress = progress;
    }

    @Override
    protected void compute() {

        if (size > BATCH_SIZE) {

            final long mid = (low + high) >>> 1;
            invokeAll(
                    new TeleportationAction(from, to, low, mid, progress),
                    new TeleportationAction(from, to, mid, high, progress)
            );
        } else {

            final ArrayList<InsertOneModel<Document>> data = from
                    .find()
                    .skip((int) low)
                    .limit((int) size)
                    .batchSize((int) size)
                    .maxTime(
                            10,
                            TimeUnit.SECONDS
                    )
                    .map(
                            InsertOneModel::new
                    )
                    .into(
                            new ArrayList<>()
                    );


            to.bulkWrite(
                    data,
                    BULK_WRITE_OPTIONS
            );

            if (progress.isPresent()) {
                progress.get().tick(size);

                System.out.print(String.format("%s\r", progress.get().bar()));
            }

        }
    }
}
