package me.vadyalex.mongoportal.core;


import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Progress {

    public static Progress start(long total) {
        return new Progress(total);
    }

    public static final int TOTAL_SECTIONS = 90;

    private final Stopwatch stopwatch;

    private final long total;

    private AtomicLong current = new AtomicLong(0);

    public Progress(long total) {
        Preconditions.checkArgument(total > 0);

        this.total = total;
        stopwatch = Stopwatch.createStarted();

        final int totalStringLength = (int) (Math.log10(total) + 1);
    }

    public String status() {
        return String.format(
                "%s/%s in %s ms",
                current.get(),
                total,
                stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );
    }

    public String bar() {
        final int sections = (int) (TOTAL_SECTIONS * current.get() / total);

        return new StringBuilder()
                .append("[")
                .append(
                        sections - 1 <= 0 ?
                                ""
                                :
                                Strings.repeat(
                                        "=",
                                        sections - 1
                                )
                )
                .append(">")
                .append(
                        Strings.repeat(" ",
                                sections == 0 ?
                                        TOTAL_SECTIONS - sections - 1
                                        :
                                        TOTAL_SECTIONS - sections
                        )
                )
                .append("] ")
                .append(
                        String.format("%s/%s", current.get(), total)
/*
                        TODO this is from Apache Utils
                        Strings.center(
                                String.format("%s/%s", current.get(), total),
                                String.valueOf(total).length() * 2 + 1
                        )
*/
                )
                .toString();

    }

    public long tick() {
        return tick(1);
    }

    public long tick(long count) {
        Preconditions.checkArgument(current.get() + count <= total, "Progress can not be larger than total");

        if (current.get() < total)
            current.getAndAdd(count);

        if (current.get() == total && stopwatch.isRunning()) {
            stopwatch.stop();
        }

        return current.get();
    }

}
