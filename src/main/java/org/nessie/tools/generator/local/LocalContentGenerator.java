package org.nessie.tools.generator.local;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.nessie.tools.generator.ContentGenerator;
import org.apache.hadoop.conf.Configuration;

import picocli.CommandLine;

@CommandLine.Command(name = "local", mixinStandardHelpOptions = true,
        description = "Generate the tables using the local path as warehouse for Iceberg metadata and data files")
public class LocalContentGenerator extends ContentGenerator {

    @Override
    protected void putObject(String localFileLocation, String remoteLocation) {
        try {
            Files.copy(Path.of(localFileLocation), Path.of(remoteLocation));
        } catch (FileAlreadyExistsException e) {
            // Possibly from previous runs
            System.out.println("Template file already exists, reusing it - " + remoteLocation);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void remoteCopy(String remoteBaseLocation, String remoteDestLocation) {
        try {
            Path remoteLocationPath = Path.of(remoteDestLocation);
            if (Files.notExists(remoteLocationPath.getParent())) {
                Files.createDirectories(remoteLocationPath.getParent());
            }

            Files.copy(Path.of(remoteBaseLocation), remoteLocationPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected FileIO io() throws IOException {
        FileUtils.forceMkdir(new File(warehousePath));
        return new HadoopFileIO(new Configuration());
    }
}
