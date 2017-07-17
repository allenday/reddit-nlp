package com.allenday;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by the exercise pipelines.
 */
public interface Options extends DataflowPipelineOptions {
	
  @Description("BigQuery query to use (standard sql).")
  String getQuery();
  void setQuery(String value);
  
  @Description("Pub/Sub topic to read from. Used if --query is empty.")
  String getTopic();
  void setTopic(String value);
  
  @Description("BigQuery Dataset to write tables to. Must already exist.")
  @Validation.Required
  String getOutputDataset();
  void setOutputDataset(String value);
  
  @Description("The BigQuery input table name. Should not already exist.")
  @Validation.Required
  String getOutputTableName();
  void setOutputTableName(String value);
}
