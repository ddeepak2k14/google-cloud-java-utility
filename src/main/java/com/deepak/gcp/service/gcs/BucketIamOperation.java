package com.deepak.gcp.service.gcs;

import java.util.Map;
import java.util.Set;

import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.Storage;

/**
 * 
 * @author DeepakKumar
 *
 */
public class BucketIamOperation {
	
	private Storage storage;
	public BucketIamOperation() {
		storage = CloudStorageConnector.getGCSFromDefaultCredential();
	}

  /** listing the Bucket-Level IAM Roles and Members */
  public Policy listBucketIamMembers(String bucketName) {
    Policy policy = storage.getIamPolicy(bucketName);
    Map<Role, Set<Identity>> policyBindings = policy.getBindings();
    for (Map.Entry<Role, Set<Identity>> entry : policyBindings.entrySet()) {
      System.out.printf("Role: %s Identities: %s\n", entry.getKey(), entry.getValue());
    }
    return policy;
  }

  /** adding a member to the Bucket-level IAM */
  public Policy addBucketIamMember(String bucketName, Role role, Identity identity) {
    Policy policy = storage.getIamPolicy(bucketName);
    Policy updatedPolicy =storage.setIamPolicy(bucketName, policy.toBuilder().addIdentity(role, identity).build());
    if (updatedPolicy.getBindings().get(role).contains(identity)) {
      System.out.printf("Added %s with role %s to %s\n", identity, role, bucketName);
    }
    return updatedPolicy;
  }

  /** Example of removing a member from the Bucket-level IAM */
  public Policy removeBucketIamMember(String bucketName, Role role, Identity identity) {
    Policy policy = storage.getIamPolicy(bucketName);
    Policy updatedPolicy = storage.setIamPolicy(bucketName, policy.toBuilder().removeIdentity(role, identity).build());
    if (updatedPolicy.getBindings().get(role) == null
        || !updatedPolicy.getBindings().get(role).contains(identity)) {
      System.out.printf("Removed %s with role %s from %s\n", identity, role, bucketName);
    }
    return updatedPolicy;
  }
}
