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

    public FileSink(Path filePath) {
        try {
            logger = Logger.getLogger("SECURITY_AUDIT");

            fileHandler = new FileHandler(filePath.toString(), true);

            logger.addHandler(fileHandler);
            Formatter formatter = new EventFormatter();
            fileHandler.setFormatter(formatter);
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
