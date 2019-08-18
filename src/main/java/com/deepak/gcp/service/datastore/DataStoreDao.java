package com.deepak.gcp.service.datastore;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.deepak.gcp.service.datastore.model.ToDoTask;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;

/**
 * 
 * @author DeepakKumar
 *
 */

public class DataStoreDao {
	Datastore datastore;
	private final String kind;
    private final KeyFactory keyFactory;
	
	public DataStoreDao(DataStoreConnector datastoreConnector) {
		datastore = DataStoreConnector.getDataStoreFromDefaultCredential();	
		kind="ToDoTask";
		keyFactory = datastore.newKeyFactory().setKind(kind);
		
	}
	 public List<ToDoTask> getAll() {
	        EntityQuery.Builder queryBuilder = Query.newEntityQueryBuilder()
	                .setKind(kind)
	                .setOrderBy(StructuredQuery.OrderBy.asc("taskName"));
	        QueryResults<Entity> entities = datastore.run(queryBuilder.build());
	        
	        return DataStoreUtil.covertToList(entities);
	    }
	    public List<ToDoTask> getInProgressTask() {
	    	EntityQuery.Builder queryBuilder = Query.newEntityQueryBuilder()
	                .setKind(kind)
	                .setFilter(StructuredQuery.PropertyFilter.eq("isActive", true));
	        QueryResults<Entity> entities = datastore.run(queryBuilder.build());
	        return DataStoreUtil.covertToList(entities);
	    }
	    

	    public Optional<ToDoTask> getByTaskId(String id) {
	        return datastore.fetch(keyFactory.newKey(Long.parseLong(id)))
	                .stream()
	                .filter(Objects::nonNull)
	                .findFirst()
	                .map(DataStoreUtil::convertFromEntitytoTask);
	    }

	    public void insert(ToDoTask task) {
	        if (!findByName(task.getTaskName()).isPresent()) {
	            Key key = datastore.allocateId(keyFactory.newKey());
	            Entity.Builder entity = Entity.newBuilder(key);
	            entity.set("taskId", key.getId().toString());
	            DataStoreUtil.convertFromToDoTaskToEntity(entity, task);
	            datastore.add(entity.build());
	        }
	    }

	    public void update(ToDoTask oldToDoTask, ToDoTask newToDoTask) {
	        datastore.fetch(keyFactory.newKey(oldToDoTask.getTaskId()))
	                .stream()
	                .filter(Objects::nonNull)
	                .findFirst()
	                .map(old -> {
	                    Entity.Builder entity = Entity.newBuilder(old);

	                    DataStoreUtil.convertFromToDoTaskToEntity(entity, newToDoTask);

	                    return entity.build();
	                })
	                .ifPresent(datastore::update);
	    }
	    
	    public Optional<ToDoTask> findByName(String name) {
	        EntityQuery.Builder queryBuilder = Query.newEntityQueryBuilder()
	                .setKind(kind)
	                .setFilter(StructuredQuery.PropertyFilter.eq("taskName", name))
	                .setLimit(1);
	        QueryResults<Entity> entities = datastore.run(queryBuilder.build());
	        return Optional.ofNullable(entities)
	                .filter(Iterator::hasNext)
	                .map(Iterator::next)
	                .map(DataStoreUtil::convertFromEntitytoTask);
	    }

	    public void deleteTask(String id) {
	    	getByTaskId(id).ifPresent(r -> {
	            datastore.delete(keyFactory.newKey(Long.parseLong(id)));
	        });
	    }
	
	

}
