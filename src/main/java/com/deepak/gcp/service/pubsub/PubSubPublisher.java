package com.deepak.gcp.service.pubsub;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

/**
 * 
 * @author DeepakKumar
 *
 */
public class PubSubPublisher {
	
	private static final String PROJECT_ID = "my-project";
	private static final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private static final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
	private static final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
	  //to use efault project id
	//private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
	Publisher publisher = null;
	
	
	public static void createTopic() throws Exception {
	    ProjectTopicName topic = ProjectTopicName.of("my-project-id", "my-topic-id");
	    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
	      topicAdminClient.createTopic(topic);
	    }
	  }
	
	
	public void publishMessage2() throws InterruptedException, ExecutionException, IOException {
    String topicId = "my-topic";
    ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
    Publisher publisher = null;
    List<ApiFuture<String>> futures = new ArrayList<>();
    try {
    	publisher = Publisher.newBuilder(topicName)
    		  .setCredentialsProvider(FixedCredentialsProvider
    		  .create(ServiceAccountCredentials.fromStream(new FileInputStream(new File("service-account-json")))))
    		  .build();
        String message = "message";
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(data)
            .build();

        // Schedule a message to be published. Messages are automatically batched.
        ApiFuture<String> future = publisher.publish(pubsubMessage);
        futures.add(future);
    } finally {
      // Wait on any pending requests
      List<String> messageIds = ApiFutures.allAsList(futures).get();

      for (String messageId : messageIds) {
        System.out.println(messageId);
      }

      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
      }
    }
  }
	
public void publishMessage1() throws FileNotFoundException, IOException {
	publisher = Publisher.newBuilder("topicName")
  		  .setCredentialsProvider(FixedCredentialsProvider
  				  .create(ServiceAccountCredentials.fromStream(new FileInputStream(new File("service-account-json"))))).build();
	  String msgData = new ObjectMapper().writeValueAsString(new Object());
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(msgData)).build();
      publisher.publish(pubsubMessage);
	
}

public void publishMessage() throws JsonProcessingException {
	 String msgData = new ObjectMapper().writeValueAsString(new Object());
     PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(msgData)).build();
     publisher.publish(pubsubMessage);
	
}

/*public Publisher getPublisher() throws IOException {
    readLock.lock();
    try {
        if(null != publisher) {
            return publisher;
        }
    } finally {
        readLock.unlock();
    }

    writeLock.lock();
    try {
        publisher = publisherBuilder.build();
    } finally {
        writeLock.unlock();
    }
    return publisher;
}*/
}
