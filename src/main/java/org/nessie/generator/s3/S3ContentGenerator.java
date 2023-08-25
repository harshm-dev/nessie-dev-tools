package org.nessie.generator.s3;

import java.io.File;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.nessie.generator.ContentGenerator;

import com.amazonaws.services.s3.AmazonS3URI;

import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@CommandLine.Command(name = "s3", mixinStandardHelpOptions = true,
        description = "Generate the tables using S3 as the filesystem store for Iceberg metadata and data files")
public class S3ContentGenerator extends ContentGenerator {

    @CommandLine.Option(names = {"--aws-access-key", "-ak"}, required = true, order = 101, description = {"AWS account's access key." +
            "Generate using AWS IAM console (https://console.aws.amazon.com/iam/). Example: AKIAIOSFODNN7EXAMPLE"})
    protected String accessKey;

    @CommandLine.Option(names = {"--aws-secret-key", "-as"}, required = true, order = 102, description = {"AWS account's secret key." +
            "Generate using AWS IAM console (https://console.aws.amazon.com/iam/). Example: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
    protected String secretKey;

    @CommandLine.Option(names = {"--aws-region-code", "-ar"}, defaultValue = "us-west-2",
            description = {"AWS region to be used, defaults to 'us-west-2'. Use reference (https://www.aws-services.info/regions.html)"})
    protected String region;

    private S3Client s3Client;

    @Override
    protected void setup() {
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        s3Client = S3Client.builder().region(Region.of(region)).credentialsProvider(credentialsProvider).build();
        super.setup();
    }

    @Override
    protected void putObject(String localFileLocation, String remoteLocation) {
        AmazonS3URI uri = new AmazonS3URI(remoteLocation);
        s3Client.putObject(PutObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(),
                RequestBody.fromFile(new File(localFileLocation)));
    }

    @Override
    protected void remoteCopy(String remoteBaseLocation, String remoteDestLocation) {
        AmazonS3URI remoteSourceUri = new AmazonS3URI(remoteBaseLocation);
        String source = String.join("/", remoteSourceUri.getBucket(), remoteSourceUri.getKey());
        AmazonS3URI remoteDestUri = new AmazonS3URI(remoteDestLocation);

        s3Client.copyObject(CopyObjectRequest.builder().copySource(source).destinationBucket(remoteDestUri.getBucket())
                .destinationKey(remoteDestUri.getKey()).build());
    }

    @Override
    protected FileIO io() {
        return new S3FileIO(() -> s3Client);
    }
}
