/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.util.Locale;

final class PostgresIdentifier {

    private PostgresIdentifier() {
    }

    static String routineName(String identifier) {
        String name = lastPart(identifier.trim());
        if (name.length() >= 2 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
            return name.substring(1, name.length() - 1).replace("\"\"", "\"");
        }
        return name.toLowerCase(Locale.ROOT);
    }

    private static String lastPart(String identifier) {
        boolean quoted = false;
        int lastDot = -1;
        for (int i = 0; i < identifier.length(); i++) {
            char character = identifier.charAt(i);
            if (character == '"') {
                if (quoted && i + 1 < identifier.length() && identifier.charAt(i + 1) == '"') {
                    i++;
                }
                else {
                    quoted = !quoted;
                }
            }
            else if (character == '.' && !quoted) {
                lastDot = i;
            }
        }
        return identifier.substring(lastDot + 1).trim();
    }
}
