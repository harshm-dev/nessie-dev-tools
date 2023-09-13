package org.nessie.generator;

import org.nessie.generator.local.LocalContentGenerator;
import org.nessie.generator.s3.S3ContentGenerator;

import picocli.CommandLine;

@CommandLine.Command(name = "iceberg-content-generator", mixinStandardHelpOptions = true,
        subcommands = {S3ContentGenerator.class, LocalContentGenerator.class}, showDefaultValues = true)
public class CLI {
    public static void main(String args[]) {
        CommandLine commandLine = new CommandLine(new CLI());
        commandLine.setUsageHelpWidth(150);

        commandLine.execute(args);
    }
}
