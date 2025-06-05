package org.example.lfs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class S3LfsServerTests {

    private S3Client mockS3Client;
    private S3LfsServer s3LfsServer;
    private final String BUCKET_NAME = "test-lfs-bucket";

    @BeforeEach
    void setUp() {
        mockS3Client = mock(S3Client.class);
        s3LfsServer = new S3LfsServer(BUCKET_NAME, mockS3Client);
    }

    private String generateOid(String content) {
        // Simple pseudo-OID generator for testing, not cryptographically secure
        return Integer.toHexString(content.hashCode());
    }

    private InputStream createInputStream(String data) {
        return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testGetObjectSuccess() throws IOException {
        String content = "This is LFS object content.";
        String oid = generateOid(content);
        long size = content.getBytes(StandardCharsets.UTF_8).length;
        String s3Key = "lfs/objects/" + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;

        GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(size).build();
        ResponseInputStream<GetObjectResponse> responseInputStream =
            new ResponseInputStream<>(getObjectResponse, createInputStream(content));

        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> {
            GetObjectRequest req = invocation.getArgument(0);
            assertEquals(BUCKET_NAME, req.bucket());
            assertEquals(s3Key, req.key());
            return responseInputStream;
        });

        InputStream resultStream = s3LfsServer.getObject(oid, size);
        assertNotNull(resultStream);
        byte[] resultContent = resultStream.readAllBytes();
        assertEquals(content, new String(resultContent, StandardCharsets.UTF_8));
        resultStream.close();
    }

    @Test
    void testGetObjectNotFound() {
        String oid = "nonexistentoid123";
        long size = 100L;
        String s3Key = "lfs/objects/" + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;

        when(mockS3Client.getObject(any(GetObjectRequest.class)))
            .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        IOException exception = assertThrows(IOException.class, () -> {
            s3LfsServer.getObject(oid, size);
        });
        assertTrue(exception.getMessage().contains("LFS object not found in S3"));
    }

    @Test
    void testPutObjectSuccess() throws IOException {
        String content = "Another LFS object.";
        String oid = generateOid(content);
        long size = content.getBytes(StandardCharsets.UTF_8).length;
        InputStream dataStream = createInputStream(content);
        String s3Key = "lfs/objects/" + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;

        ArgumentCaptor<PutObjectRequest> putRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        when(mockS3Client.putObject(putRequestCaptor.capture(), requestBodyCaptor.capture()))
            .thenReturn(PutObjectResponse.builder().build());

        s3LfsServer.putObject(oid, size, dataStream);

        PutObjectRequest capturedRequest = putRequestCaptor.getValue();
        assertEquals(BUCKET_NAME, capturedRequest.bucket());
        assertEquals(s3Key, capturedRequest.key());
        assertEquals(size, capturedRequest.contentLength());

        RequestBody capturedBody = requestBodyCaptor.getValue();
        assertNotNull(capturedBody);
        // Further verification of RequestBody content is complex with mocks,
        // but we can check if it was created with the correct size.
        assertEquals(size, capturedBody.contentLength());
    }

    @Test
    void testVerifyObjectExistsAndSizeMatches() throws IOException {
        String oid = "existingoid123";
        long size = 123L;
        String s3Key = "lfs/objects/" + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;

        HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(size).build();
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
            HeadObjectRequest req = invocation.getArgument(0);
            assertEquals(BUCKET_NAME, req.bucket());
            assertEquals(s3Key, req.key());
            return headResponse;
        });

        assertTrue(s3LfsServer.verifyObject(oid, size));
    }

    @Test
    void testVerifyObjectExistsSizeMismatch() throws IOException {
        String oid = "sizemismatchoid";
        long actualSizeInS3 = 200L;
        long expectedSizeInVerify = 100L;
        String s3Key = "lfs/objects/" + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;

        HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(actualSizeInS3).build();
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headResponse);

        assertFalse(s3LfsServer.verifyObject(oid, expectedSizeInVerify));
    }

    @Test
    void testVerifyObjectNotExists() throws IOException {
        String oid = "nonexistentoid456";
        long size = 100L;
        String s3Key = "lfs/objects/" + oid.substring(0, 2) + "/" + oid.substring(2, 4) + "/" + oid;

        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
            .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        assertFalse(s3LfsServer.verifyObject(oid, size));
    }

    @Test
    void testGetObjectS3Error() {
        String oid = "erroroid789";
        long size = 50L;
        when(mockS3Client.getObject(any(GetObjectRequest.class)))
            .thenThrow(S3Exception.builder().message("S3 Access Denied").statusCode(403).build());

        IOException exception = assertThrows(IOException.class, () -> {
            s3LfsServer.getObject(oid, size);
        });
        assertTrue(exception.getMessage().contains("S3 error"));
    }
}
