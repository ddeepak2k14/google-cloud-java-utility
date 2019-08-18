package com.deepak.gcp.service.firestore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

/**
 * 
 * @author DeepakKumar
 *
 */
public class FireStoreConnector {
	private static Firestore firestore;
	public Firestore getDataStoreFromDefaultCredential(){
		// for Default Project
		//firestore = FirestoreOptions.getDefaultInstance().toBuilder().build().getService();
		firestore = FirestoreOptions.getDefaultInstance().toBuilder().setProjectId("projectId").build().getService();
        return firestore;
	}
	
	public Firestore getDatastoreFromCredentialFile() throws IOException {
		GoogleCredentials credentials;
		File credentialsPath = new File("service_account.json");
		FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
		credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		firestore = FirestoreOptions
	            .newBuilder()
	            .setProjectId("")
	            .setCredentials(credentials)
	            .build()
	            .getService();
		return firestore;
	}
    
	

}
