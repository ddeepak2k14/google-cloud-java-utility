package com.deepak.gcp.service.pubsub;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;

/**
 * 
 * @author DeepakKumar
 *
 */
public class PubSubSubscriber{
	private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
	private static final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();
	
	public void cretaeSubscription() throws IOException {
		ProjectTopicName topic = ProjectTopicName.of("my-project-id", "my-topic-id");
	    ProjectSubscriptionName subscription =  ProjectSubscriptionName.of("my-project-id", "my-subscription-id");
	    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
	      subscriptionAdminClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance(), 0);
	    }
	}
	
	static class MessageReceiverClient implements MessageReceiver {
	    @Override
	    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
	      messages.offer(message);
	      consumer.ack();
	    }
	  }
	public static void readFromPubSub() throws InterruptedException, IOException {
		 String subscriptionId = "subscriptionname";
		    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, subscriptionId);
		    Subscriber subscriber = null;
		    try {
		      // create a subscriber bound to the asynchronous message receiver
		      subscriber = Subscriber.newBuilder(subscriptionName, new MessageReceiverClient()).build();
		      subscriber.startAsync().awaitRunning();
		      // Continue to listen to messages
		      while (true) {
		        PubsubMessage message = messages.take();
		        System.out.println("Message Id: " + message.getMessageId());
		        System.out.println("Data: " + message.getData().toStringUtf8());
		      }
		    } finally {
		      if (subscriber != null) {
		        subscriber.stopAsync();
		      }
		    }
	}
	
	public void readFromPubSub1() throws FileNotFoundException, IOException {
		String messagePayload;
		String subscriptionId = "";
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, subscriptionId);
		MessageReceiver receiver = new MessageReceiver() {
			@Override
			public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
				String messagePayload = message.getData().toStringUtf8();
				System.out.println(messagePayload);
				//Process your message 
				//after successful processing send acknowledgement
				consumer.ack();
			}
		}; 
		
		/*MessageReceiver receiver1 = (message, consumer) ->{
			String messagePayload2=null;
			boolean sendAck = true;
			try {
				messagePayload2 = message.getData().toStringUtf8();
				//Process your message , after successful processing send acknowledgement
			}
			catch(Exception e) {
				sendAck = false;
				
			}
			finally {
				if(sendAck) {
					consumer.ack();
				}
			}
			
		};*/
		
		 Subscriber.Builder builder = Subscriber.newBuilder(subscriptionName, receiver);
	        // provide a separate executor service for polling
	        ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(2).build();

	        builder.setCredentialsProvider(FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(new File("service-account-json")))));
	       //use default credential configured or from cloud sdk default login
	       // builder.setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()));
	        Subscriber subscriber = builder.setExecutorProvider(executorProvider).build();
	        subscriber.addListener(new Subscriber.Listener() {
	            @Override
	            public void failed(Subscriber.State from, Throwable failure) {
	                // Handle failure. This is called when the Subscriber encountered a fatal error
	                // and is shutting down.
	            }
	        }, MoreExecutors.directExecutor());
	        subscriber.startAsync().awaitRunning();
	}
}
