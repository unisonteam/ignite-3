package org.apache.ignite.internal.eventlog.sink.file;

import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.ignite.internal.eventlog.Event;
import org.apache.ignite.internal.eventlog.sink.Sink;

public class FileSink implements Sink {
    private Logger logger;
    private FileHandler fileHandler;

    public FileSink(Path workDir, String filePattern, String name) {
        try {
            logger = Logger.getLogger(name);
            fileHandler = new FileHandler(workDir.toAbsolutePath() + filePattern.toString(), true);
            logger.addHandler(fileHandler);
            fileHandler.setFormatter(new EventFormatter());
        } catch (SecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void log(String message) {
        logger.info(message);
    }

    @Override
    public void write(Event event) {
        log(event.toString());
    }

    @Override
    public void flush() {

    }


    private static class EventFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            return record.getMessage();
        }
    }
}
