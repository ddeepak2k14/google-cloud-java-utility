package com.deepak.gcp.service.bigquery;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.JobListOption;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;

/**
 * 
 * @author DeepakKumar
 *
 */
public class BigQueryUtil {
	private BigQuery bigquery;
	
	public BigQueryUtil() {
		bigquery = BigQueryConnector.getBQFromDefaultCredential();
		
	}
	
	/** Example of creating a dataset. */
	  public Dataset createDataset(String datasetName) {
	    Dataset dataset = null;
	    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
	    try {
	      dataset = bigquery.create(datasetInfo);
	      System.out.println(" Dataset Created ");
	    } catch (BigQueryException e) {
	      System.out.println("the dataset was not created");
	    }
	    return dataset;
	  }
	  
	  /** Example of listing datasets in a project, specifying the page size. */
	  public Page<Dataset> listDatasets(String projectId) {
	    Page<Dataset> datasets = bigquery.listDatasets(projectId, DatasetListOption.pageSize(100));
	    for (Dataset dataset : datasets.iterateAll()) {
	      // do something with the dataset
	    }
	    return datasets;
	  }

	  /** Example of deleting a dataset from its id, even if non-empty. */
	  public boolean deleteDataset(String datasetName) {
	    boolean deleted = bigquery.delete(datasetName, DatasetDeleteOption.deleteContents());
	    if (deleted) {
	      // the dataset was deleted
	    } else {
	      // the dataset was not found
	    }
	    return deleted;
	  }

	  /** Example of deleting a dataset, even if non-empty. */
	  public boolean deleteDatasetFromId(String projectId, String datasetName) {
	    DatasetId datasetId = DatasetId.of(projectId, datasetName);
	    boolean deleted = bigquery.delete(datasetId, DatasetDeleteOption.deleteContents());
	    if (deleted) {
	      // the dataset was deleted
	    } else {
	      // the dataset was not found
	    }
	    return deleted;
	  }

	  /** Example of deleting a table. */
	  public boolean deleteTable(String datasetName, String tableName) {
	    TableId tableId = TableId.of(datasetName, tableName);
	    boolean deleted = bigquery.delete(tableId);
	    if (deleted) {
	      // the table was deleted
	    } else {
	      // the table was not found
	    }
	    return deleted;
	  }

	  /** Example of deleting a table. */
	  public boolean deleteTableFromId(String projectId, String datasetName, String tableName) {
	    TableId tableId = TableId.of(projectId, datasetName, tableName);
	    boolean deleted = bigquery.delete(tableId);
	    if (deleted) {
	      // the table was deleted
	    } else {
	      // the table was not found
	    }
	    return deleted;
	  }

	  /** Example of listing the tables in a dataset, specifying the page size. */
	  public Page<Table> listTables(String datasetName) {
	    // [START ]
	    Page<Table> tables = bigquery.listTables(datasetName, TableListOption.pageSize(100));
	    for (Table table : tables.iterateAll()) {
	      // do something with the table
	    }
	    // [END ]
	    return tables;
	  }

	  /** Example of listing the tables in a dataset. */
	  public Page<Table> listTablesFromId(String projectId, String datasetName) {
	    DatasetId datasetId = DatasetId.of(projectId, datasetName);
	    Page<Table> tables = bigquery.listTables(datasetId, TableListOption.pageSize(100));
	    for (Table table : tables.iterateAll()) {
	      // do something with the table
	    }
	    return tables;
	  }

	  /** Example of getting a dataset. */
	  public Dataset getDataset(String datasetName) {
	    Dataset dataset = bigquery.getDataset(datasetName);
	    return dataset;
	  }

	  /** Example of getting a dataset. */
	  public Dataset getDatasetFromId(String projectId, String datasetName) {
	    DatasetId datasetId = DatasetId.of(projectId, datasetName);
	    Dataset dataset = bigquery.getDataset(datasetId);
	    return dataset;
	  }

	  /** Example of getting a table. */
	  public Table getTable(String datasetName, String tableName) {
	    Table table = bigquery.getTable(datasetName, tableName);
	    return table;
	  }

	  /** Example of getting a table. */
	  public Table getTableFromId(String projectId, String datasetName, String tableName) {
	    TableId tableId = TableId.of(projectId, datasetName, tableName);
	    Table table = bigquery.getTable(tableId);
	    return table;
	  }
	  
	  /** Example of writing a local file to a table. */
	  // [TARGET writer(WriteChannelConfiguration)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  // [VARIABLE FileSystems.getDefault().getPath(".", "my-data.csv")]
	  // [VARIABLE "us"]
	  public long writeFileToTable(String datasetName, String tableName, Path csvPath, String location)
	      throws IOException, InterruptedException, TimeoutException {
	    TableId tableId = TableId.of(datasetName, tableName);
	    WriteChannelConfiguration writeChannelConfiguration =
	        WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
	    // Generally, location can be inferred based on the location of the referenced dataset.
	    // However,
	    // it can also be set explicitly to force job execution to be routed to a specific processing
	    // location.  See https://cloud.google.com/bigquery/docs/locations for more info.
	    JobId jobId = JobId.newBuilder().setLocation(location).build();
	    TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
	    // Write data to writer
	    try (OutputStream stream = Channels.newOutputStream(writer)) {
	      Files.copy(csvPath, stream);
	    } finally {
	      writer.close();
	    }
	    // Get load job
	    Job job = writer.getJob();
	    job = job.waitFor();
	    LoadStatistics stats = job.getStatistics();
	    return stats.getOutputRows();
	  }

	  /** Example of loading a newline-delimited-json file with textual fields from GCS to a table. */
	  // [TARGET create(JobInfo, JobOption...)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  public Long writeRemoteFileToTable(String datasetName, String tableName)
	      throws InterruptedException {
	    String sourceUri = "gs://cloud-samples-data/bigquery/us-states/us-states.json";
	    TableId tableId = TableId.of(datasetName, tableName);
	    Field[] fields =
	        new Field[] {
	          Field.of("name", LegacySQLTypeName.STRING),
	          Field.of("post_abbr", LegacySQLTypeName.STRING)
	        };
	    // Table schema definition
	    Schema schema = Schema.of(fields);
	    LoadJobConfiguration configuration =
	        LoadJobConfiguration.builder(tableId, sourceUri)
	            .setFormatOptions(FormatOptions.json())
	            .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	            .setSchema(schema)
	            .build();
	    // Load the table
	    Job loadJob = bigquery.create(JobInfo.of(configuration));
	    loadJob = loadJob.waitFor();
	    System.out.println("State: " + loadJob.getStatus().getState());
	    return ((StandardTableDefinition) bigquery.getTable(tableId).getDefinition()).getNumRows();
	  }

	  /** Example of inserting rows into a table without running a load job. */
	  // [TARGET insertAll(InsertAllRequest)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  public InsertAllResponse insertAll(String datasetName, String tableName) {
	    TableId tableId = TableId.of(datasetName, tableName);
	    // Values of the row to insert
	    Map<String, Object> rowContent = new HashMap<>();
	    rowContent.put("booleanField", true);
	    // Bytes are passed in base64
	    rowContent.put("bytesField", "Cg0NDg0="); // 0xA, 0xD, 0xD, 0xE, 0xD in base64
	    // Records are passed as a map
	    Map<String, Object> recordsContent = new HashMap<>();
	    recordsContent.put("stringField", "Hello, World!");
	    rowContent.put("recordField", recordsContent);
	    InsertAllResponse response =
	        bigquery.insertAll(
	            InsertAllRequest.newBuilder(tableId)
	                .addRow("rowId", rowContent)
	                // More rows can be added in the same RPC by invoking .addRow() on the builder
	                .build());
	    if (response.hasErrors()) {
	      // If any of the insertions failed, this lets you inspect the errors
	      for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
	        // inspect row error
	      }
	    }
	    return response;
	  }

	  /** Example of creating a table. */
	  // [TARGET create(TableInfo, TableOption...)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  // [VARIABLE "string_field"]
	  public Table createTable(String datasetName, String tableName, String fieldName) {
	    // [START bigquery_create_table]
	    TableId tableId = TableId.of(datasetName, tableName);
	    // Table field definition
	    Field field = Field.of(fieldName, LegacySQLTypeName.STRING);
	    // Table schema definition
	    Schema schema = Schema.of(field);
	    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
	    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
	    Table table = bigquery.create(tableInfo);
	    // [END bigquery_create_table]
	    return table;
	  }

	  /** Example of listing table rows, specifying the page size. */
	  // [TARGET listTableData(String, String, TableDataListOption...)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  public TableResult listTableData(String datasetName, String tableName) {
	    // [START ]
	    // This example reads the result 100 rows per RPC call. If there's no need to limit the number,
	    // simply omit the option.
	    TableResult tableData =
	        bigquery.listTableData(datasetName, tableName, TableDataListOption.pageSize(100));
	    for (FieldValueList row : tableData.iterateAll()) {
	      // do something with the row
	    }
	    // [END ]
	    return tableData;
	  }

	  /** Example of listing table rows, specifying the page size. */
	  // [TARGET listTableData(TableId, TableDataListOption...)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  public TableResult listTableDataFromId(String datasetName, String tableName) {
	    // [START bigquery_browse_table]
	    TableId tableIdObject = TableId.of(datasetName, tableName);
	    // This example reads the result 100 rows per RPC call. If there's no need to limit the number,
	    // simply omit the option.
	    TableResult tableData =
	        bigquery.listTableData(tableIdObject, TableDataListOption.pageSize(100));
	    for (FieldValueList row : tableData.iterateAll()) {
	      // do something with the row
	    }
	    // [END bigquery_browse_table]
	    return tableData;
	  }

	  /** Example of listing table rows with schema. */
	  // [TARGET listTableData(String, String, Schema, TableDataListOption...)]
	  // [VARIABLE "my_dataset_name"]
	  // [VARIABLE "my_table_name"]
	  // [VARIABLE ...]
	  // [VARIABLE "field"]
	  public TableResult listTableDataSchema(
	      String datasetName, String tableName, Schema schema, String field) {
	    // [START ]
	    TableResult tableData = bigquery.listTableData(datasetName, tableName, schema);
	    for (FieldValueList row : tableData.iterateAll()) {
	      row.get(field);
	    }
	    // [END ]
	    return tableData;
	  }

	  /** Example of listing table rows with schema. */
	  // [TARGET listTableData(TableId, Schema, TableDataListOption...)]
	  public FieldValueList listTableDataSchemaId() {
	    // [START ]
	    Schema schema =
	        Schema.of(
	            Field.of("word", LegacySQLTypeName.STRING),
	            Field.of("word_count", LegacySQLTypeName.STRING),
	            Field.of("corpus", LegacySQLTypeName.STRING),
	            Field.of("corpus_date", LegacySQLTypeName.STRING));
	    TableResult tableData =
	        bigquery.listTableData(
	            TableId.of("bigquery-public-data", "samples", "shakespeare"), schema);
	    FieldValueList row = tableData.getValues().iterator().next();
	    System.out.println(row.get("word").getStringValue());
	    // [END ]
	    return row;
	  }

	  /** Example of creating a query job. */
	  // [TARGET create(JobInfo, JobOption...)]
	  // [VARIABLE "SELECT field FROM my_dataset_name.my_table_name"]
	  public Job createJob(String query) {
	    // [START ]
	    Job job = null;
	    JobConfiguration jobConfiguration = QueryJobConfiguration.of(query);
	    JobInfo jobInfo = JobInfo.of(jobConfiguration);
	    try {
	      job = bigquery.create(jobInfo);
	    } catch (BigQueryException e) {
	      // the job was not created
	    }
	    // [END ]
	    return job;
	  }

	  /** Example of listing jobs, specifying the page size. */
	  // [TARGET listJobs(JobListOption...)]
	  public Page<Job> listJobs() {
	    // [START bigquery_list_jobs]
	    Page<Job> jobs = bigquery.listJobs(JobListOption.pageSize(100));
	    for (Job job : jobs.iterateAll()) {
	      // do something with the job
	    }
	    // [END bigquery_list_jobs]
	    return jobs;
	  }

	  /** Example of getting a job. */
	  // [TARGET getJob(String, JobOption...)]
	  // [VARIABLE "my_job_name"]
	  public Job getJob(String jobName) {
	    // [START ]
	    Job job = bigquery.getJob(jobName);
	    if (job == null) {
	      // job was not found
	    }
	    // [END ]
	    return job;
	  }

	  /** Example of getting a job. */
	  // [TARGET getJob(JobId, JobOption...)]
	  // [VARIABLE "my_job_name"]
	  public Job getJobFromId(String jobName) {
	    // [START ]
	    JobId jobIdObject = JobId.of(jobName);
	    Job job = bigquery.getJob(jobIdObject);
	    if (job == null) {
	      // job was not found
	    }
	    // [END ]
	    return job;
	  }

	  /** Example of cancelling a job. */
	  // [TARGET cancel(String)]
	  // [VARIABLE "my_job_name"]
	  public boolean cancelJob(String jobName) {
	    // [START ]
	    boolean success = bigquery.cancel(jobName);
	    if (success) {
	      // job was cancelled
	    } else {
	      // job was not found
	    }
	    // [END ]
	    return success;
	  }

	  /** Example of cancelling a job. */
	  // [TARGET cancel(JobId)]
	  // [VARIABLE "my_job_name"]
	  public boolean cancelJobFromId(String jobName) {
	    // [START ]
	    JobId jobId = JobId.of(jobName);
	    boolean success = bigquery.cancel(jobId);
	    if (success) {
	      // job was cancelled
	    } else {
	      // job was not found
	    }
	    // [END ]
	    return success;
	  }

	  /** Example of running a query. */
	  public void runQuery() throws InterruptedException {
	    String query = "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus;";
	    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
	    for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
	      for (FieldValue val : row) {
	        System.out.printf("%s,", val.toString());
	      }
	      System.out.printf("\n");
	    }
	  }
	  
	  

}
