package com.deepak.gcp.service.bigquery;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

/**
 * 
 * @author DeepakKumar
 *
 */
public class BigQueryConnector {
	private static BigQuery bigquery;
	
	public static BigQuery getBQFromDefaultCredential() {
		//default project configuration
		//bigquery = BigQueryOptions.getDefaultInstance().getService();
		bigquery = BigQueryOptions.getDefaultInstance().toBuilder().setProjectId("projectId").build().getService();
		return bigquery ;
	}
	
	public static BigQuery getBQFromCredentialFile() throws FileNotFoundException, IOException {
		GoogleCredentials credentials;
		File credentialsPath = new File("service_account.json");
		FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
		credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		bigquery =BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("projectId").build().getService();
		return bigquery;
	} 
}
