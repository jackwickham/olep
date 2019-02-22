package net.jackw.olep.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jackw.olep.common.DatabaseConfig;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class DiskMetrics extends Metrics {
    /**
     * The data is being written fast, so reduce the number of flushes by increasing the buffer size
     */
    private static final int WRITE_BUFFER_SIZE = 65536;

    private BufferedWriter durationWriter;
    private BufferedWriter eventWriter;

    public DiskMetrics(String mainClass, String resultsDirName, DatabaseConfig config) throws IOException {
        // First make the results dir
        File resultsDir = new File(resultsDirName);
        if (!resultsDir.exists() && !resultsDir.mkdirs()) {
            throw new IOException("Failed to create results dir " + resultsDir.getPath());
        }

        // Create the file of duration events
        durationWriter = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(new File(resultsDir, mainClass + "-durations.csv")), StandardCharsets.UTF_8
        ), WRITE_BUFFER_SIZE);
        durationWriter.write("t,event,duration\n");

        // And the file of instantaneous events
        eventWriter = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(new File(resultsDir, mainClass + "-events.csv")), StandardCharsets.UTF_8
        ), WRITE_BUFFER_SIZE);
        eventWriter.write("t,event,data\n");

        // Also copy the config file to the results dir, to allow easy checking that it is the correct benchmark
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(new File(resultsDir, "config.json"), config);

        // Close the files once we finish
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                durationWriter.close();
                eventWriter.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    /**
     * Record a duration
     *
     * @param type The type of duration event that this corresponds to
     * @param timer The timer that was started when the event started
     */
    @Override
    public void recordDuration(DurationType type, Timer timer) {
        long duration = timer.getElapsed();
        String line = System.currentTimeMillis() + "," + type.toString() + "," + duration + "\n";
        try {
            durationWriter.write(line);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Record an instantaneous event
     *
     * @param type The type of event that is being recorded
     * @param data Additional data about the event
     */
    @Override
    public void recordEvent(EventType type, @Nonnull String data) {
        String line = System.currentTimeMillis() + "," + type.toString() + "," + data + "\n";
        try {
            eventWriter.write(line);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
