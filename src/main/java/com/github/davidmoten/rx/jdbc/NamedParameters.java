package com.github.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.List;

public class NamedParameters {

    public static final JdbcQuery parse(String namedSql) {
        // was originally using regular expressions, but they didn't work well
        // for ignoring parameter-like strings inside quotes.
        List<String> names = new ArrayList<String>();
        int length = namedSql.length();
        StringBuffer parsedQuery = new StringBuffer(length);
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < length; i++) {
            char c = namedSql.charAt(i);
            if (inSingleQuote) {
                if (c == '\'') {
                    inSingleQuote = false;
                }
            } else if (inDoubleQuote) {
                if (c == '"') {
                    inDoubleQuote = false;
                }
            } else {
                if (c == '\'') {
                    inSingleQuote = true;
                } else if (c == '"') {
                    inDoubleQuote = true;
                } else if (c == ':' && i + 1 < length
                        && Character.isJavaIdentifierStart(namedSql.charAt(i + 1))) {
                    int j = i + 2;
                    while (j < length && Character.isJavaIdentifierPart(namedSql.charAt(j))) {
                        j++;
                    }
                    String name = namedSql.substring(i + 1, j);
                    c = '?'; // replace the parameter with a question mark
                    i += name.length(); // skip past the end if the parameter
                    names.add(name);
                }
            }
            parsedQuery.append(c);
        }
        return new JdbcQuery(parsedQuery.toString(), names);
    }

    public static class JdbcQuery {
        private final String sql;
        private List<String> names;

        public JdbcQuery(String sql, List<String> names) {
            this.sql = sql;
            this.names = names;
            ;
        }

        public String sql() {
            return sql;
        }

        public List<String> names() {
            return names;
        }

    }

}