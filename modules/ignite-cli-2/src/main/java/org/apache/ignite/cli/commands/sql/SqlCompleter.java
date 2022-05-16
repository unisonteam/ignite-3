package org.apache.ignite.cli.commands.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.cli.sql.SqlSchemaProvider;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

class SqlCompleter implements Completer {
    private final SqlSchemaProvider schemaProvider;
    private final List<Candidate> candidates = new ArrayList<>();

    SqlCompleter(SqlSchemaProvider schemaProvider) {
        fillCandidates();
        this.schemaProvider = schemaProvider;
    }

    void refreshSchema() {
        schemaProvider.invalidateSchema();
        fillCandidates();
    }

    @Override
    public void complete(LineReader reader, ParsedLine commandLine, List<Candidate> candidates) {
        if (commandLine.wordIndex() == 0) {
            addCandidatesFromArray(SqlMetaData.STARTING_KEYWORDS, candidates);
        } else {
            candidates.addAll(this.candidates);
        }
        //todo add columns from current schema?
    }

    private void fillCandidates() {
        addKeywords();
        for (Entry<String, Map<String, Set<String>>> schema : schemaProvider.getSchema().entrySet()) {
            addCandidate(schema.getKey(), candidates); // add schema name
            for (Entry<String, Set<String>> table : schema.getValue().entrySet()) {
                addCandidate(table.getKey(), candidates); // add table name
            }
        }
    }

    private void addKeywords() {
        addCandidatesFromArray(SqlMetaData.KEYWORDS, candidates);
        addCandidatesFromArray(SqlMetaData.NUMERIC_FUNCTIONS, candidates);
        addCandidatesFromArray(SqlMetaData.STRING_FUNCTIONS, candidates);
        addCandidatesFromArray(SqlMetaData.TIME_DATE_FUNCTIONS, candidates);
        addCandidatesFromArray(SqlMetaData.SYSTEM_FUNCTIONS, candidates);
    }

    private static void addCandidatesFromArray(String[] strings, List<Candidate> candidates) {
        for (String keyword : strings) {
            addCandidate(keyword, candidates);
        }
    }

    private static void addCandidate(String string, List<Candidate> candidates) {
        candidates.add(new Candidate(string));
        candidates.add(new Candidate(string.toLowerCase()));
    }
}
