package io.leoplatform.sdk.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.SimpleStats;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.aws.AWSResources;
import io.leoplatform.sdk.bus.LoadingBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public final class S3TransferManager {
    private static final Logger log = LoggerFactory.getLogger(S3TransferManager.class);

    private final String name;
    private final TransferManager s3TransferManager;
    private final S3Results resultsProcessor;
    private final LoadingBot bot;

    private final Queue<PendingS3Upload> pendingUploads = new LinkedList<>();
    private final AtomicBoolean running;
    private final AtomicBoolean uploading;
    private final Lock lock = new ReentrantLock();
    private final Condition newUpload = lock.newCondition();

    @Inject
    public S3TransferManager(AWSResources resources, ExecutorManager executorManager,
                             S3Results resultsProcessor, LoadingBot bot) {
        this.name = resources.storage();
        this.s3TransferManager = TransferManagerBuilder.standard()
            .withS3Client(client(resources))
                .withDisableParallelDownloads(false)
                .build();
        this.resultsProcessor = resultsProcessor;
        this.bot = bot;
        running = new AtomicBoolean(true);
        uploading = new AtomicBoolean(false);
        CompletableFuture.runAsync(this::synchronousUpload, executorManager.get());
    }

    void enqueue(PendingS3Upload pendingUpload) {
        if (running.get()) {
            lock.lock();
            try {
                pendingUploads.add(pendingUpload);
                newUpload.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    void flush() {
        signalUploader();
        awaitUploader();
    }

    private void synchronousUpload() {
        while (running.get()) {
            lock.lock();
            try {
                if (pendingUploads.isEmpty()) {
                    uploading.set(false);
                    newUpload.await();
                    uploading.set(true);
                }
            } catch (InterruptedException i) {
                running.set(false);
                pendingUploads.clear();
                log.warn("S3 transfer manager stopped with {} pending", pendingUploads.size());
            } finally {
                lock.unlock();
            }
            while (!pendingUploads.isEmpty()) {
                uploadNext();
            }
        }
        uploading.set(false);
        log.info("Transfer manager stopped");
    }

    private void uploadNext() {
        PendingS3Upload next;
        lock.lock();
        try {
            next = pendingUploads.remove();
        } catch (Exception e) {
            log.warn("Unexpectedly empty upload queue");
            return;
        } finally {
            lock.unlock();
        }
        upload(next);
    }

    private void upload(PendingS3Upload next) {
        log.info("Beginning upload of {} to S3", next.filename());
        PutObjectRequest request = next.s3PutRequest(name);
        Upload upload = s3TransferManager.upload(request);
        try {
            UploadResult uploadResult = upload.waitForUploadResult();
            S3Payload s3Payload = next.s3Payload(uploadResult, bot.name());
            log.info("{} byte upload of {} complete", s3Payload.getGzipSize(), next.filename());
            resultsProcessor.addSuccess(s3Payload, uploadResult);
        } catch (Exception e) {
            log.warn("S3 upload unexpectedly stopped");
            running.set(false);
            resultsProcessor.addFailure(upload.getDescription(), e);
        }
    }

    StreamStats end() {
        flush();
        running.set(false);
        signalUploader();
        s3TransferManager.shutdownNow();
        return getStats();
    }

    private void awaitUploader() {
        while (!pendingUploads.isEmpty() || uploading.get()) {
            signalUploader();
            lock.lock();
            try {
                newUpload.await(100, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("S3 transfer manager unexpectedly stopped");
                pendingUploads.clear();
            } finally {
                lock.unlock();
            }
        }
    }

    private void signalUploader() {
        lock.lock();
        try {
            newUpload.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private AmazonS3 client(AWSResources resources) {
        return AmazonS3ClientBuilder
                .standard()
            .withCredentials(resources.credentials())
                .build();
    }

    private StreamStats getStats() {
        return new SimpleStats(resultsProcessor.successes(), resultsProcessor.failures(), resultsProcessor.start());
    }
}
