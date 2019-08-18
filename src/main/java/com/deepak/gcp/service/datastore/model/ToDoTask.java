package com.deepak.gcp.service.datastore.model;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UnknownFormatConversionException;

import com.google.cloud.datastore.Entity;
/**
 * 
 * @author DeepakKumar
 *
 */

public class ToDoTask {
	private String taskName;
	private Long taskId;
	private Boolean isActive;
	private OffsetDateTime createdOn;
	
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	
	public Long getTaskId() {
		return taskId;
	}
	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}
	
	public OffsetDateTime getCreatedOn() {
		return createdOn;
	}
	public void setCreatedOn(OffsetDateTime createdOn) {
		this.createdOn = createdOn;
	}
	
	public Boolean getIsActive() {
		return isActive;
	}
	public void setIsActive(Boolean isActive) {
		this.isActive = isActive;
	}
	
	
	

}
