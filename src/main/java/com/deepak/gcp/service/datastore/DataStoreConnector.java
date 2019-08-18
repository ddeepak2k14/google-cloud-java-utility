package com.deepak.gcp.service.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;

/**
 * 
 * @author DeepakKumar
 *
 */
public class DataStoreConnector {
	private static Datastore datastore;
	
	public static Datastore getDataStoreFromDefaultCredential(){
		//for default project
		//datastore = DatastoreOptions.getDefaultInstance().toBuilder().build().getService();
		
		datastore = DatastoreOptions.getDefaultInstance().toBuilder().setProjectId("projectId").build().getService();
		EntityQuery.Builder queryBuilder = Query.newEntityQueryBuilder()
                .setKind("user");
        QueryResults<Entity> entities = datastore.run(queryBuilder.build());
        System.out.println("successfull" + entities);
        return datastore;
	}
	
	public static Datastore getDatastoreFromCredentialFile() throws IOException {
		GoogleCredentials credentials;
		File credentialsPath = new File("service_account.json");
		FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
		credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		datastore = DatastoreOptions
	            .newBuilder()
	            .setProjectId("")
	            .setCredentials(credentials)
	            .setNamespace("")
	            .build()
	            .getService();
		return datastore;
	}
    
	

}
