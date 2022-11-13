package io.github.reata.sqllineage4j.cli.utils;

import org.apache.commons.cli.CommandLine;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public final class Helper {
    public static String extractSqlFromArgs(CommandLine cmd) {
        StringBuilder sql = new StringBuilder();
        if (cmd.getOptionValue("f") != null) {
            String file = cmd.getOptionValue("f");
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line = reader.readLine();
                while (line != null) {
                    sql.append(line);
                    sql.append(System.lineSeparator());
                    line = reader.readLine();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (IOException e) {
                System.exit(1);
            }
        } else if (cmd.getOptionValue("e") != null) {
            sql.append(cmd.getOptionValue("e"));
        }
        return sql.toString();
    }
}
