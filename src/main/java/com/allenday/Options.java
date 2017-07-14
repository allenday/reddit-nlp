package com.allenday;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by the exercise pipelines.
 */
public interface Options extends DataflowPipelineOptions {

	/*
  @Description("Path to the data file(s) containing game data.")
  String getInput();

  void setInput(String value);

  @Description("Pub/Sub topic to read from. Used if --input is empty.")
  String getTopic();

  void setTopic(String value);
  */

	@Description("BigQuery Dataset to read from. Must already exist.")
  @Validation.Required
  String getInputDataset();
  void setInputDataset(String value);

  @Description("The BigQuery input table name. Must already exist.")
  @Validation.Required
  String getInputTableName();
  void setInputTableName(String value);

  @Description("BigQuery Dataset to write tables to. Must already exist.")
  @Validation.Required
  String getOutputDataset();
  void setOutputDataset(String value);
  
  @Description("The BigQuery input table name. Should not already exist.")
  @Validation.Required
  String getOutputTableName();
  void setOutputTableName(String value);
}
