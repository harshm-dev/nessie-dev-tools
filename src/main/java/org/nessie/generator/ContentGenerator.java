package org.nessie.generator;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.GC_ENABLED;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.apache.iceberg.types.Types;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;

import picocli.CommandLine;
import software.amazon.awssdk.utils.ImmutableMap;

public abstract class ContentGenerator implements Runnable {

    private static final String TABLE_PREFIX = "table";
    private static final String BASE_FILE = "base.parquet";

    @CommandLine.Option(names = {"--nessie-uri"}, order = 1, defaultValue = "http://localhost:19120/api/v2", description = {"Http URI path for Nessie, defaults to http://localhost:19120/api/v2"})
    protected URI nessieUri;

    @CommandLine.Option(names = {"--warehouse", "-w"}, required = true, order = 4, description = {"Warehouse location, where table files will be stored."})
    protected String warehousePath;

    @CommandLine.Option(names = {"--tables-count", "-tc"}, defaultValue = "100", order = 2, description = {"Number of tables to be generated, defaults to 100"})
    protected int noOfTables;

    @CommandLine.Option(names = {"--snapshots-count", "-ts"}, defaultValue = "2", order = 3, description = {"Number of snapshots to be generated, defaults to 2"})
    protected int noOfSnapshots;

    private NessieCatalog nessieIcebergCatalog;
    private NessieApiV2 nessieApi;
    private FileIO io;
    private Path templateDataFileLocalPath;
    private final Random random = new Random();

    protected void setup() {
        try {
            io = io();
            setupNessieApi();
            setupNessieIcebergCatalog();
            setupTemplateDataFile();
        } catch (IOException nfe) {
            System.err.println("Unable to setup content generator - " + nfe.getMessage());
            throw new RuntimeException((nfe));
        }
    }

    private void setupNessieApi() {
        this.nessieApi = HttpClientBuilder.builder().withUri(nessieUri).build(NessieApiV2.class);
    }

    private void setupNessieIcebergCatalog() throws NessieNotFoundException {
        nessieIcebergCatalog = new NessieCatalog();
        Branch defaultRef = nessieApi.getDefaultBranch();
        NessieIcebergClient nessieIcebergClient = new NessieIcebergClient(nessieApi, defaultRef.getName(),
                defaultRef.getHash(), new HashMap<>());
        nessieIcebergCatalog.initialize("gentool", nessieIcebergClient, io,
                ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehousePath));
    }

    private void setupTemplateDataFile() throws IOException {
        try (final InputStream baseFileStream = getClass().getClassLoader().getResourceAsStream(BASE_FILE)) {
            templateDataFileLocalPath = Files.createTempDirectory("content_generator").resolve("template_data.parquet");
            Files.copy(baseFileStream, templateDataFileLocalPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    protected abstract void putObject(String localFileLocation, String remoteLocation);

    protected abstract void remoteCopy(String remoteBaseLocation, String remoteDestLocation);

    protected abstract FileIO io() throws IOException;

    @Override
    public void run() {
        setup();

        try {
            String defaultBranchName = nessieApi.getDefaultBranch().getName();
            AtomicInteger countDown = new AtomicInteger(noOfTables);
            long startTime = System.currentTimeMillis();

            String templateDataFileRemotePath = warehousePath + "/template_data.parquet";
            putObject(templateDataFileLocalPath.toString(), templateDataFileRemotePath);
            String prefix = TABLE_PREFIX + random.nextInt(100);

            IntStream.range(0, noOfTables).parallel().forEach(t -> {
                Table table = createTable(prefix + t, defaultBranchName);
                createSnapshots(table, noOfSnapshots, templateDataFileRemotePath);
                table.updateProperties().set(GC_ENABLED, "true").set(COMMIT_NUM_RETRIES, "4").commit();
                System.out.println("Generated " + table.name());

                if (countDown.decrementAndGet() % 10 == 0) {
                    System.out.println("Remaining: " + countDown.get());
                }
            });

            System.out.println("Time taken: " + (System.currentTimeMillis() - startTime)/1000 + " seconds");
            io.deleteFile(templateDataFileRemotePath);
        } catch (Exception e) {
            System.err.println("Unable to generate the tables on this env - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private Table createTable(String tableName, String branchName) {
        List<Types.NestedField> fields = Stream.of(
                Types.NestedField.required(0, "id", new Types.IntegerType()))
                .collect(Collectors.toList());
        Schema icebergTableSchema = new Schema(fields);
        String table1QualifiedName = String.format("%s@%s", tableName, branchName);
        return nessieIcebergCatalog.createTable(TableIdentifier.of(table1QualifiedName), icebergTableSchema);
    }

    private void createSnapshots(Table table, int count, String templateDataFile) {
        String tableLocation = table.location();

        for (int i = 0; i < count; i++) {
            String dataFile = String.format("%s/data/data_%d.parquet", tableLocation, i);
            remoteCopy(templateDataFile, dataFile);

            table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                    .withPath(dataFile)
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(559L)
                    .withRecordCount(1L)
                    .build()).commit();
            table.refresh();
        }
    }
}
