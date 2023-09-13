package org.nessie.tools.generator;

import org.nessie.tools.generator.local.LocalContentGenerator;
import org.nessie.tools.generator.s3.S3ContentGenerator;

import picocli.CommandLine;

@CommandLine.Command(name = "generate-tables", mixinStandardHelpOptions = true,
        description = "Generates test tables on the Nessie catalog.",
        subcommands = {S3ContentGenerator.class, LocalContentGenerator.class}, showDefaultValues = true)
public class ContentGenCLI {

}
