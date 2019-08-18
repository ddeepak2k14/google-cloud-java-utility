package com.deepak.gcp.service.gcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * 
 * @author DeepakKumar
 *
 */
public class CloudStorageConnector {
	private static Storage storage;
	
	public static Storage getGCSFromDefaultCredential() {
		storage = StorageOptions.getDefaultInstance().getService();
		return storage;
	}
	
	public static Storage getGCSFromCredentialFile() throws IOException {
		GoogleCredentials credentials;
		File credentialsPath = new File("service_account.json");
		FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
		credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
		return storage;
	}
	

}
