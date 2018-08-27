package com.leo.sdk.aws.s3;


import java.util.Comparator;

public class S3BatchComparator implements Comparator<S3Batch> {
    @Override
    public int compare(S3Batch b1, S3Batch b2) {

        //TODO: fix this
        return Comparator.comparing(S3Batch::getAge)
                .thenComparing(S3Batch::getBatchRecords)
                .thenComparing(S3Batch::getBatchSize)
                .thenComparing(S3Batch::getBatchRecords)
                .thenComparing(S3Batch::getUploadAttempts)
                .compare(b1, b2);
    }
}
