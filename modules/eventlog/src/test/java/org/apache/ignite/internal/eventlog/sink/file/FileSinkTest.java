package org.apache.ignite.internal.eventlog.sink.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.eventlog.Event;
import org.apache.ignite.internal.eventlog.EventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSinkTest {

    @Test
    void writeToFile(@TempDir Path tmp) throws IOException {
        // Given file path that does not exists.
        Path path = Path.of(tmp.toUri()).resolve("fileSink.log");
        assertThat(path.toFile().exists(), equalTo(false));

        // When create a new FileSink with the file path.
        FileSink fileSink = new FileSink(tmp, "fileSink.log", "test-sink");
        // And write event.
        fileSink.write(new CountEvent(1));

        // Then the file should be created.
        assertThat(path.toFile().exists(), equalTo(true));
        // And event is written to the file.
        assertThat(Files.readAllLines(path), equalTo(List.of(new CountEvent(1).toString())));
    }

    private static class CountEvent implements Event {
       private final int cnt;

        private CountEvent(int cnt) {
            this.cnt = cnt;
        }

        public int cnt() {
            return cnt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CountEvent that = (CountEvent) o;
            return cnt == that.cnt;
        }

        @Override
        public String toString() {
            return "CountEvent{" +
                    "cnt=" + cnt +
                    '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(cnt);
        }

        @Override
        public EventType type() {
            return EventType.AUTHENTICATION; // fixme
        }
    }
}