package org.apache.ignite.cli.commands.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.cli.sql.SqlManager;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

class SqlCompleter implements Completer {
    private final SqlManager sqlManager;
    private final List<Candidate> candidates = new ArrayList<>();

    SqlCompleter(SqlManager sqlManager) {
        this.sqlManager = sqlManager;
        fillCandidates();
    }

    void refreshSchema() {
        sqlManager.invalidateSchema();
        fillCandidates();
    }

    @Override
    public void complete(LineReader reader, ParsedLine commandLine, List<Candidate> candidates) {
        candidates.addAll(this.candidates);
        //todo add columns from current schema?
    }

    private void fillCandidates() {
        addKeywords();
        for (Entry<String, Map<String, Set<String>>> schema : sqlManager.getSchema().entrySet()) {
            addCandidate(schema.getKey(), candidates); // add schema name
            for (Entry<String, Set<String>> table : schema.getValue().entrySet()) {
                addCandidate(table.getKey(), candidates); // add table name
            }
        }
    }

    private void addKeywords() {
        addCandidatesFromArray(SqlMetaData.KEYWORDS);
        addCandidatesFromArray(SqlMetaData.NUMERIC_FUNCTIONS);
        addCandidatesFromArray(SqlMetaData.STRING_FUNCTIONS);
        addCandidatesFromArray(SqlMetaData.TIME_DATE_FUNCTIONS);
        addCandidatesFromArray(SqlMetaData.SYSTEM_FUNCTIONS);
    }

    private void addCandidatesFromArray(String[] strings) {
        for (String keyword : strings) {
            addCandidate(keyword, candidates);
        }
    }

    private static void addCandidate(String string, List<Candidate> candidates) {
        candidates.add(new Candidate(string));
        candidates.add(new Candidate(string.toLowerCase()));
    }
}
