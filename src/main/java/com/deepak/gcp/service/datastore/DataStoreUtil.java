package com.deepak.gcp.service.datastore;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UnknownFormatConversionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.deepak.gcp.service.datastore.model.ToDoTask;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.QueryResults;

/**
 * 
 * @author DeepakKumar
 *
 */
public class DataStoreUtil {
	public static ToDoTask convertFromEntitytoTask(Entity entity) {
		ToDoTask task = new ToDoTask();
		if (entity.contains("taskId")) {
	        	task.setTaskId(entity.getLong("taskId"));
	    }else {
	        throw new UnknownFormatConversionException(String.format("entity is missing taskId: %s", entity.toString()));
	    }
		
	    if (entity.contains("taskName")) {
	        	task.setTaskName(entity.getString("taskName"));
	    }else {
	        throw new UnknownFormatConversionException(String.format("entity is missing taskName: %s", entity.toString()));
	    }
	    
	    if (entity.contains("createdOn")) {
	        	task.setCreatedOn(OffsetDateTime.ofInstant(entity.getTimestamp("createdOn").toDate().toInstant(), ZoneId.of("America/New_York")));
	    }
	    
	    if (entity.contains("isActive")) {
	        	task.setIsActive(entity.getBoolean("isActive"));
	    }
	    return task;
	}
	
	   public static void convertFromToDoTaskToEntity(Entity.Builder entity, ToDoTask ToDoTask) {
	        entity.set("name", ToDoTask.getTaskName());
	        Optional<OffsetDateTime> createdOn = Optional.ofNullable(ToDoTask.getCreatedOn());
	        if (createdOn.isPresent()) {
	            entity.set("createdOn", Timestamp.of(new Date(createdOn.get().toEpochSecond())));
	        } else {
	            entity.set("createdOn", NullValue.of());
	        }
	    }
	   
	   public static List<ToDoTask> covertToList(QueryResults<Entity> entities){
		 //before java 8
		   /*List<ToDoTask> todoList = new ArrayList<>();
	        while(entities.hasNext()) {
	        	ToDoTask todoTask = DataStoreUtil.convertFromEntitytoTask(entities.next());
	        	todoList.add(todoTask);
	        }
	        return todoList;*/
		   return StreamSupport
	                .stream(Spliterators.spliteratorUnknownSize(entities, Spliterator.ORDERED), false)
	                .map(DataStoreUtil::convertFromEntitytoTask)
	                .collect(Collectors.toList());
	   }

}
