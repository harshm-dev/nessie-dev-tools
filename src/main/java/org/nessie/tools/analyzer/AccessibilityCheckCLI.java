package org.nessie.tools.analyzer;

import org.nessie.tools.analyzer.s3.S3AccessibilityCheck;

import picocli.CommandLine;

@CommandLine.Command(name = "check-accessibility", mixinStandardHelpOptions = true,
        description = "Runs through the catalog and checks if the tables are accessible. Identifies the absent or denied access metadata.",
        subcommands = {S3AccessibilityCheck.class})
public class AccessibilityCheckCLI {
}
