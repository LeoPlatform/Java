package io.leoplatform.sdk.aws.s3;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.model.UploadResult;

public interface PendingS3Upload {
    String filename();

    PutObjectRequest s3PutRequest(String name);

    S3Payload s3Payload(UploadResult result, String botName);
}
