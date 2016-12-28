package me.vadyalex.mongoportal.core;


import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class ProgressTest {

    public static final Logger LOGGER = LoggerFactory.getLogger(ProgressTest.class);

    @Test
    public void check() {

        final Progress progress = Progress.start(25);

        LOGGER.info(
                progress.bar()
        );

        Assertions.assertThat(progress.bar()).startsWith("[>                                                                                         ]");

        progress.tick(11);

        LOGGER.info(
                progress.bar()
        );

        Assertions.assertThat(progress.bar()).startsWith("[======================================>                                                   ]");

        progress.tick(4);

        LOGGER.info(
                progress.bar()
        );

        progress.tick(10);

        LOGGER.info(
                progress.bar()
        );

        Assertions.assertThat(progress.bar()).startsWith("[=========================================================================================>]");

        LOGGER.info(
                progress.status()
        );

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incorrect_total() {
        Progress.start(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void progress_out_of_bounds() {
        final Progress progress = Progress.start(10);

        progress.tick(5);
        progress.tick(5);

        progress.tick(5);
    }

}
