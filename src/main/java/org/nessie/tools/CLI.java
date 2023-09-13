package org.nessie.tools;

import org.nessie.tools.analyzer.AccessibilityCheckCLI;
import org.nessie.tools.generator.ContentGenCLI;

import picocli.CommandLine;

@CommandLine.Command(name = "nessie-dev-tools", mixinStandardHelpOptions = true,
        subcommands = {ContentGenCLI.class, AccessibilityCheckCLI.class}, showDefaultValues = true)
public class CLI {
    public static void main(String args[]) {
        CommandLine commandLine = new CommandLine(new CLI());
        commandLine.setUsageHelpWidth(150);

        commandLine.execute(args);
    }
}
