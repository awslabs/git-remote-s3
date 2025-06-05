package org.example.lfs;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.io.IOException;

public class S3LfsServer {

    private final S3Client s3Client;
    private final String bucketName;
    private final String lfsPrefix = "lfs/objects/"; // Define how LFS objects are stored

    public S3LfsServer(String bucketName, S3Client s3Client) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    private String getS3Key(String oid) {
        // Example: lfs/objects/ab/cd/abcdef12345...
        // This helps in partitioning objects in S3
        if (oid == null || oid.length() < 4) {
            throw new IllegalArgumentException("OID must be at least 4 characters long");
        }
        return lfsPrefix + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;
    }

    /**
     * Retrieves an LFS object from S3.
     * @param oid The object ID.
     * @param size The expected size of the object (can be used for validation).
     * @return InputStream of the object data.
     * @throws IOException if the object is not found or an S3 error occurs.
     */
    public InputStream getObject(String oid, long size) throws IOException {
        String key = getS3Key(oid);
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);

            // Optionally, you can validate the size if 'size' parameter is reliable
            // GetObjectResponse response = s3Object.response();
            // if (response.contentLength() != size) {
            //     s3Object.close(); // Important to close the stream
            //     throw new IOException("Object size mismatch for OID: " + oid + ". Expected: " + size + ", Found: " + response.contentLength());
            // }

            return s3Object;
        } catch (NoSuchKeyException e) {
            throw new IOException("LFS object not found in S3: " + oid + " (key: " + key + ")", e);
        } catch (S3Exception e) {
            throw new IOException("S3 error while getting LFS object " + oid + ": " + e.getMessage(), e);
        } catch (SdkException e) {
            throw new IOException("AWS SDK error while getting LFS object " + oid + ": " + e.getMessage(), e);
        }
    }

    /**
     * Uploads an LFS object to S3.
     * @param oid The object ID.
     * @param size The size of the object.
     * @param data InputStream containing the object data.
     * @throws IOException if an S3 error occurs during upload.
     */
    public void putObject(String oid, long size, InputStream data) throws IOException {
        String key = getS3Key(oid);
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentLength(size)
                    // Potentially add metadata, e.g., "oid": oid
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(data, size));
        } catch (S3Exception e) {
            throw new IOException("S3 error while putting LFS object " + oid + ": " + e.getMessage(), e);
        } catch (SdkException e) {
            throw new IOException("AWS SDK error while putting LFS object " + oid + ": " + e.getMessage(), e);
        }
    }

    /**
     * Checks if an LFS object exists in S3 and verifies its size.
     * @param oid The object ID.
     * @param size The expected size of the object.
     * @return true if the object exists and size matches, false otherwise.
     * @throws IOException if an S3 error occurs (other than NoSuchKeyException for verification).
     */
    public boolean verifyObject(String oid, long size) throws IOException {
        String key = getS3Key(oid);
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            var headResponse = s3Client.headObject(headObjectRequest);
            return headResponse.contentLength() == size;
        } catch (NoSuchKeyException e) {
            // Object does not exist, which is a valid outcome for verification
            return false;
        } catch (S3Exception e) {
            // For other S3 errors (access denied, etc.), rethrow as IOException
            throw new IOException("S3 error while verifying LFS object " + oid + ": " + e.getMessage(), e);
        } catch (SdkException e) {
            throw new IOException("AWS SDK error while verifying LFS object " + oid + ": " + e.getMessage(), e);
        }
    }
}
