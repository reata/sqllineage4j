package io.github.reata.sqllineage4j.cli;

import io.github.reata.sqllineage4j.core.LineageRunner;
import org.apache.commons.cli.*;

import static io.github.reata.sqllineage4j.cli.utils.Helper.extractSqlFromArgs;

public class SQLLineage4j {

    public static void main(String[] args) {
        Options options = new Options();
        Option exec = Option.builder("e").argName("quoted-query-string").hasArg().desc("SQL from command line").build();
        Option file = Option.builder("f").argName("filename").hasArg().desc("SQL from files").build();
        Option verbose = Option.builder("v").longOpt("verbose").desc("increase output verbosity, show statement level lineage result").build();
        options.addOption(exec);
        options.addOption(file);
        options.addOption(verbose);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("e") && cmd.hasOption("f")) {
                System.out.println("Both -e and -f options are specified. -e option will be ignored");
            }
            if (cmd.hasOption("e") || cmd.hasOption("f")) {
                String sql = extractSqlFromArgs(cmd);
                LineageRunner runner = cmd.hasOption("v") ? LineageRunner.builder(sql).verbose().build() : LineageRunner.builder(sql).build();
                runner.printTableLineage();
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("sqllineage4j", options);
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
