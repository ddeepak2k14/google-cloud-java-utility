package com.deepak.gcp.service.gcs;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Role;
import com.google.cloud.storage.Acl.User;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;

/**
 * 
 * @author DeepakKumar
 *
 */
public class GCSUtil {
	private Storage storage;
	public GCSUtil() {
		storage = CloudStorageConnector.getGCSFromDefaultCredential();
	}
	
	public Bucket createBucket(String bucketName) {
	    Bucket bucket = storage.create(BucketInfo.of(bucketName));
	    return bucket;
	  }
	 
	  public boolean isBucketExists() {
	    boolean exists = createBucket("my-bucket").exists();
	    if (exists) {
	     System.out.println("Bucket Exists");
	    } else {
	      System.out.println("Bucket was not found");
	    }
	    return exists;
	  }
	
	 /** creating a bucket with storage class and location. */
	  public Bucket createBucketWithStorageClassAndLocation(String bucketName) {
	    Bucket bucket =
	        storage.create(
	            BucketInfo.newBuilder(bucketName)
	                .setStorageClass(StorageClass.COLDLINE)
	                .setLocation("asia")
	                .build());
	    return bucket;
	  }
	  
	  public Page<Bucket> listBucketsWithSizeAndPrefix(String prefix) {
		    Page<Bucket> buckets = storage.list(BucketListOption.pageSize(100), BucketListOption.prefix(prefix));
		    for (Bucket bucket : buckets.iterateAll()) {
		      // do something with the bucket
		    }
		    return buckets;
		  }
	  
	  public List<Blob> listObjectsInBucket(String bucketName) throws IOException, GeneralSecurityException {
			Bucket bucket  = storage.get(bucketName);
			for (Blob blob : bucket.list().iterateAll()) {
				System.out.println("Process/Download Blob: "+ blob);
			};
			return StreamSupport
					.stream(Spliterators.spliteratorUnknownSize(bucket.list().iterateAll().iterator(), Spliterator.ORDERED), false)
					.collect(Collectors.toList());
		}
	  
	
	  public void downloadFile(String bucketName, String srcFilename, Path destFilePath)throws IOException {
		//Path destFilePath = Paths.get("/local/path/to/file.txt")
	    Blob blob = storage.get(BlobId.of(bucketName, srcFilename));
	    blob.downloadTo(destFilePath);
	  }
	  
	  /** Example of getting the ACL entry for an entity on a bucket. */
	  public Acl getBucketAcl(String bucketName) {
	    Acl acl = storage.getAcl(bucketName, User.ofAllAuthenticatedUsers());
	    return acl;
	  }

	  /** Example of getting the ACL entry for a specific user on a bucket. */
	  // [email "google-cloud-java-tests@java-docs-samples-tests.iam.gserviceaccount.com"]
	  public Acl getBucketAcl(String bucketName, String userEmail) {
	    Acl acl = storage.getAcl(bucketName, new User(userEmail));
	    return acl;
	  }

	  /** Example of deleting the ACL entry for an entity on a bucket. */
	  public boolean deleteBucketAcl(String bucketName) {
	    boolean deleted = storage.deleteAcl(bucketName, User.ofAllAuthenticatedUsers());
	    if (deleted) {
	      // the acl entry was deleted
	    } else {
	      // the acl entry was not found
	    }
	    return deleted;
	  }

	  /** Example of creating a new ACL entry on a bucket. */
	  public Acl createBucketAcl(String bucketName) {
	    Acl acl = storage.createAcl(bucketName, Acl.of(User.ofAllAuthenticatedUsers(), Role.READER));
	    return acl;
	  }

	  /** Example of updating a new ACL entry on a bucket. */
	  public Acl updateBucketAcl(String bucketName) {
	    Acl acl = storage.updateAcl(bucketName, Acl.of(User.ofAllAuthenticatedUsers(), Role.OWNER));
	    return acl;
	  }

	  /** Example of listing the ACL entries for a blob. */
	  public List<Acl> listBucketAcls(String bucketName) {
	    List<Acl> acls = storage.listAcls(bucketName);
	    for (Acl acl : acls) {
	      // do something with ACL entry
	    }
	    return acls;
	  }

	  /** Example of getting the default ACL entry for an entity on a bucket. */
	  public Acl getDefaultBucketAcl(String bucketName) {
	    Acl acl = storage.getDefaultAcl(bucketName, User.ofAllAuthenticatedUsers());
	    return acl;
	  }

	  /** Example of deleting the default ACL entry for an entity on a bucket. */
	  public boolean deleteDefaultBucketAcl(String bucketName) {
	    boolean deleted = storage.deleteDefaultAcl(bucketName, User.ofAllAuthenticatedUsers());
	    if (deleted) {
	      // the acl entry was deleted
	    } else {
	      // the acl entry was not found
	    }
	    return deleted;
	  }

	  /** Example of creating a new default ACL entry on a bucket. */
	  public Acl createDefaultBucketAcl(String bucketName) {
	    Acl acl =
	        storage.createDefaultAcl(bucketName, Acl.of(User.ofAllAuthenticatedUsers(), Role.READER));
	    return acl;
	  }

	  /** Example of updating a new default ACL entry on a bucket. */
	  public Acl updateDefaultBucketAcl(String bucketName) {
	    Acl acl =
	        storage.updateDefaultAcl(bucketName, Acl.of(User.ofAllAuthenticatedUsers(), Role.OWNER));
	    return acl;
	  }

	  /** Example of listing the default ACL entries for a blob. */
	  public List<Acl> listDefaultBucketAcls(String bucketName) {
	    List<Acl> acls = storage.listDefaultAcls(bucketName);
	    for (Acl acl : acls) {
	      // do something with ACL entry
	    }
	    return acls;
	  }
	  
	/** Example of displaying Bucket metadata */
	  public void getBucketMetadata(String bucketName) throws StorageException {
	    // Select all fields
	    // Fields can be selected individually e.g. Storage.BucketField.NAME or BucketField.LIFECYCLE
	    Bucket bucket = storage.get(bucketName, BucketGetOption.fields(BucketField.values()));
	    if (bucket.getLabels() != null) {
	      for (Map.Entry<String, String> label : bucket.getLabels().entrySet()) {
	        System.out.println(label.getKey() + "=" + label.getValue());
	      }
	    }
	  }
	  
	  public Bucket updateBucket(String bucketName) {
	    BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName).setVersioningEnabled(true).build();
	    Bucket bucket = storage.update(bucketInfo);
	    return bucket;
	  }
	  
	  /** get bucket policy */
	  public Bucket getBucketPolicyOnly(String bucketName) throws StorageException {
	    Bucket bucket = storage.get(bucketName, BucketGetOption.fields(BucketField.IAMCONFIGURATION));
	    BucketInfo.IamConfiguration iamConfiguration = bucket.getIamConfiguration();
	    Boolean enabled = iamConfiguration.isBucketPolicyOnlyEnabled();
	    Date lockedTime = new Date(iamConfiguration.getBucketPolicyOnlyLockedTime());
	    if (enabled != null && enabled) {
	      System.out.println("Bucket Policy Only is enabled for " + bucketName);
	      System.out.println("Bucket will be locked on " + lockedTime);
	    } else {
	      System.out.println("Bucket Policy Only is disabled for " + bucketName);
	    }
	    return bucket;
	  }
	  
	  /** enable Bucket Policy Only for a bucket */
	  public Bucket enableBucketPolicyOnly(String bucketName) throws StorageException {
	    BucketInfo.IamConfiguration iamConfiguration = BucketInfo.IamConfiguration.newBuilder().setIsBucketPolicyOnlyEnabled(true).build();
	    Bucket bucket = storage.update(BucketInfo.newBuilder(bucketName).setIamConfiguration(iamConfiguration).build());
	    System.out.println("Bucket Policy Only was enabled for " + bucketName);
	    return bucket;
	  }

	  /** disable Bucket Policy Only for a bucket */
	  public Bucket disableBucketPolicyOnly(String bucketName) throws StorageException {
	    BucketInfo.IamConfiguration iamConfiguration = BucketInfo.IamConfiguration.newBuilder().setIsBucketPolicyOnlyEnabled(false).build();
	    Bucket bucket =storage.update(BucketInfo.newBuilder(bucketName).setIamConfiguration(iamConfiguration).build());
	    System.out.println("Bucket Policy Only was disabled for " + bucketName);
	    return bucket;
	  }
	  
	  /** setting a retention policy on a bucket */
	  public Bucket setRetentionPolicy(String bucketName, Long retentionPeriod)throws StorageException {
	    // The retention period for objects in bucket
	    // Long retentionPeriod = 3600L; // 1 hour in seconds
	    Bucket bucketWithRetentionPolicy = storage.update(BucketInfo.newBuilder(bucketName).setRetentionPeriod(retentionPeriod).build());
	    System.out.println( "Retention period for "+ bucketName+ " is now " + bucketWithRetentionPolicy.getRetentionPeriod());
	    return bucketWithRetentionPolicy;
	  }
	  
	  
	  /** removing a retention policy on a bucket */
	  public Bucket removeRetentionPolicy(String bucketName)throws StorageException, IllegalArgumentException {
	    Bucket bucket = storage.get(bucketName, BucketGetOption.fields(BucketField.RETENTION_POLICY));
	    if (bucket.retentionPolicyIsLocked() != null && bucket.retentionPolicyIsLocked()) {
	      throw new IllegalArgumentException("Unable to remove retention period as retention policy is locked.");
	    }
	    Bucket bucketWithoutRetentionPolicy =bucket.toBuilder().setRetentionPeriod(null).build().update();
	    System.out.println("Retention period for " + bucketName + " has been removed");
	    return bucketWithoutRetentionPolicy;
	  }

	  /** Example of how to get a bucket's retention policy */
	  public Bucket getRetentionPolicy(String bucketName) throws StorageException {
	    Bucket bucket = storage.get(bucketName, BucketGetOption.fields(BucketField.RETENTION_POLICY));
	    System.out.println("Retention Policy for " + bucketName);
	    System.out.println("Retention Period: " + bucket.getRetentionPeriod());
	    if (bucket.retentionPolicyIsLocked() != null && bucket.retentionPolicyIsLocked()) {
	      System.out.println("Retention Policy is locked");
	    }
	    if (bucket.getRetentionEffectiveTime() != null) {
	      System.out.println("Effective Time: " + new Date(bucket.getRetentionEffectiveTime()));
	    }
	    return bucket;
	  }
	  
	  /** Example of creating a blob with no content. */
	  public Blob createBlob(String bucketName, String blobName) {
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
	    Blob blob = storage.create(blobInfo);
	    return blob;
	  }
	  
	  /** Example of creating a blob from a byte array. */
	  public Blob createBlobFromByteArray(String bucketName, String blobName) {
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
	    Blob blob = storage.create(blobInfo, "Hello, World!".getBytes(UTF_8));
	    return blob;
	  }

	  /** Example of creating a blob with sub array from a byte array. */
	  public Blob createBlobWithSubArrayFromByteArray(String bucketName, String blobName, int offset, int length) {
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
	    Blob blob = storage.create(blobInfo, "Hello, World!".getBytes(UTF_8), offset, length);
	    return blob;
	  }
	  
	  /** Example of creating a blob from an input stream. */
	  public Blob createBlobFromInputStream(String bucketName, String blobName) {
	    InputStream content = new ByteArrayInputStream("Hello, World!".getBytes(UTF_8));
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
	    Blob blob = storage.create(blobInfo, content);
	    return blob;
	  }
	  
	  /** Example of uploading an encrypted blob. */
	  public Blob createEncryptedBlob(String bucketName, String blobName, String encryptionKey) {
	    byte[] data = "Hello, World!".getBytes(UTF_8);

	    BlobId blobId = BlobId.of(bucketName, blobName);
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
	    Blob blob = storage.create(blobInfo, data, BlobTargetOption.encryptionKey(encryptionKey));
	    return blob;
	  }
	  
	  /** Example of deleting a blob. */
	  public boolean deleteBlob(String bucketName, String blobName) {
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    boolean deleted = storage.delete(blobId);
	    if (deleted) {
	      // the blob was deleted
	    } else {
	      // the blob was not found
	    }
	    return deleted;
	  }
	  
	  /** Example of copying a blob. */
	  public Blob copyBlob(String bucketName, String blobName, String copyBlobName) {
	    CopyRequest request =
	        CopyRequest.newBuilder().setSource(BlobId.of(bucketName, blobName))
	        .setTarget(BlobId.of(bucketName, copyBlobName)).build();
	    Blob blob = storage.copy(request).getResult();
	    return blob;
	  }
	  
	  /** Example of reading all bytes of an encrypted blob. */
	  public byte[] readEncryptedBlob(String bucketName, String blobName, String decryptionKey) {
	    byte[] content = storage.readAllBytes(bucketName, blobName, BlobSourceOption.decryptionKey(decryptionKey));
	    return content;
	  }
	  
	  /** Example of how to set a temporary hold for a blob */
	  public Blob setTemporaryHold(String bucketName, String blobName) throws StorageException {
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    Blob blob = storage.update(BlobInfo.newBuilder(blobId).setTemporaryHold(true).build());
	    System.out.println("Temporary hold was set for " + blobName);
	    return blob;
	  }
	  
	  /** Example of how to release a temporary hold for a blob */
	  public Blob releaseTemporaryHold(String bucketName, String blobName) throws StorageException {
	    BlobId blobId = BlobId.of(bucketName, blobName);
	    Blob blob = storage.update(BlobInfo.newBuilder(blobId).setTemporaryHold(false).build());
	    System.out.println("Temporary hold was released for " + blobName);
	    return blob;
	  }
	  

}
