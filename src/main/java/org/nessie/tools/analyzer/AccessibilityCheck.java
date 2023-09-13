package org.nessie.tools.analyzer;

import java.net.URI;
import java.net.URISyntaxException;

import org.nessie.tools.analyzer.s3.S3AccessibilityCheck;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;

import picocli.CommandLine;

/**
 * Runs through the catalog and checks if the tables are accessible.
 */
public abstract class AccessibilityCheck implements Runnable {

    protected enum Status {SUCCESS, NOT_FOUND, ACCESS_DENIED, ERROR;}

    @CommandLine.Option(names = {"--nessie-uri"}, order = 1, defaultValue = "http://localhost:19120/api/v2", description = {"Http URI path for Nessie, defaults to http://localhost:19120/api/v2"})
    protected URI nessieUri;

    @CommandLine.Option(names = {"--errors-only"}, defaultValue = "false",
            description = {"Only print error statements; skipping the tables which are accessible."})
    protected boolean printOnlyErrors = false;

    private NessieApiV2 nessieApi;

    protected void setup() {
        setupNessieApi();
    }

    protected abstract CheckResult check(String s3Url) throws URISyntaxException;

    private void setupNessieApi() {
        this.nessieApi = HttpClientBuilder.builder().withUri(nessieUri).build(NessieApiV2.class);
    }

    @Override
    public void run() {
        setup();
        nessieApi.getAllReferences().get().getReferences().forEach(this::checkTables);
    }

    private void checkTables(Reference branch) {
        try {
            nessieApi.getEntries().reference(branch).stream()
                    .parallel()
                    .filter(e -> Content.Type.ICEBERG_TABLE == e.getType())
                    .forEach(e -> {
                        try {
                            IcebergTable t = nessieApi.getContent().hashOnRef(branch.getHash())
                                    .key(e.getName()).get().get(e.getName()).unwrap(IcebergTable.class).get();
                            CheckResult result = check(t.getMetadataLocation());

                            if (result.status != Status.SUCCESS || !printOnlyErrors) {
                                System.out.printf("Table [%s@%s], Metadata [%s], Accessibility Status [%s]\n",
                                        String.join(".", e.getName().getElements()), branch.getName(),
                                        t.getMetadataLocation(), result.getStatus().name(), result.getMessage());
                            }
                        } catch (NessieNotFoundException | URISyntaxException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
        } catch (NessieNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected static class CheckResult {
        private final Status status;
        private final String message;

        public CheckResult(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public Status getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }
    }
}
