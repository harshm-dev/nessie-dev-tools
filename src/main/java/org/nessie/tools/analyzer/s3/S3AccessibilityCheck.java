package org.nessie.tools.analyzer.s3;

import java.net.URI;
import java.net.URISyntaxException;

import org.nessie.tools.analyzer.AccessibilityCheck;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import picocli.CommandLine;

@CommandLine.Command(name = "s3", mixinStandardHelpOptions = true,
        description = "AWS S3 specific checks for accessibility")
public class S3AccessibilityCheck extends AccessibilityCheck {

    @CommandLine.Option(names = {"--aws-access-key", "-ak"}, required = true, order = 101, description = {"AWS account's access key." +
            "Generate using AWS IAM console (https://console.aws.amazon.com/iam/). Example: AKIAIOSFODNN7EXAMPLE"})
    protected String accessKey;

    @CommandLine.Option(names = {"--aws-secret-key", "-as"}, required = true, order = 102, description = {"AWS account's secret key." +
            "Generate using AWS IAM console (https://console.aws.amazon.com/iam/). Example: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
    protected String secretKey;

    private AmazonS3Client s3Client;

    @Override
    protected void setup() {
        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        s3Client = new AmazonS3Client(creds, new ClientConfiguration());
        super.setup();
    }

    @Override
    protected CheckResult check(String s3Url) throws URISyntaxException {
        try {
            URI uri = new URI(s3Url);
            String bucketName = uri.getHost();
            String path = uri.getPath().substring(1);
            boolean isPresent = s3Client.doesObjectExist(bucketName, path);
            return isPresent ? new CheckResult(Status.SUCCESS, "Table metadata is accessible") : new CheckResult(Status.NOT_FOUND, "Table metadata not found");
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 403 || e.getMessage().contains("Access Denied")) {
                return new CheckResult(Status.ACCESS_DENIED, e.getMessage());
            } else {
                return new CheckResult(Status.ERROR, e.getMessage());
            }
        }
    }
}
