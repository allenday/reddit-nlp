package com.allenday.dataflow.reddit;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import com.allenday.Options;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

/*

mvn compile exec:java \
  -Dexec.mainClass=com.allenday.dataflow.reddit.EntitySentiment \
  -Dexec.args="\
    --project=allenday-dev \
    --tempLocation=gs://allenday-dev/tmp-dataflow-staging \
    --runner=DataflowRunner \
    --outputDataset=reddit_nlp_posts \
    --outputTableName=test2 \
  --topic=projects/allenday-dev/topics/reddit_nlp_posts \
  --streaming"

*/
public class EntitySentiment {
  /**
   * A transform to read the game events from either text files or Pub/Sub topic.
   */
  public static class ReadComments extends PTransform<PBegin, PCollection<Comment>> {
		private static final long serialVersionUID = -1558390922667000289L;

    private Options options;

    public ReadComments(Options options) {
      this.options = options;
    }

    @Override
    public PCollection<Comment> expand(PBegin begin) {
      if (options.getQuery() != null && !options.getQuery().isEmpty()) {
        return begin
            .getPipeline()
        		.apply(
        				"Read BigQuery",
        				BigQueryIO.read().fromQuery(options.getQuery()).usingStandardSql())
            .apply("GetSentiment", 
            		ParDo.of(new ParseTableRowFn()));
      } else {
        return begin
            .getPipeline()
            .apply(
            		"Read PubSub",
            		PubsubIO.readMessages().fromTopic(options.getTopic()))
            .apply("GetSentiment", 
            		ParDo.of(new ParsePubsubMessageFn()));
      }
    }
  }

  public static TableSchema getSchema() {
  	// Possible values include STRING, BYTES, INTEGER, INT64 (same as INTEGER),
  	// FLOAT, FLOAT64 (same as FLOAT), BOOLEAN, BOOL (same as BOOLEAN),
  	// TIMESTAMP, DATE, TIME, DATETIME, RECORD (where RECORD indicates that the
  	// field contains a nested schema) or STRUCT (same as RECORD).

  	List<TableFieldSchema> fields = new ArrayList<>();
  	fields.add(new TableFieldSchema().setName("id").setType("STRING"));
  	fields.add(new TableFieldSchema().setName("score").setType("FLOAT"));
  	fields.add(new TableFieldSchema().setName("magnitude").setType("FLOAT"));

  	fields.add(
  		new TableFieldSchema().setName("entity").setType("RECORD").setFields(
  			new ArrayList<TableFieldSchema>() {
  				private static final long serialVersionUID = 6403605378922542033L;
  				{
  					add(new TableFieldSchema().setName("name").setType("STRING"));
  					add(new TableFieldSchema().setName("salience").setType("FLOAT"));
  					add(new TableFieldSchema().setName("magnitude").setType("FLOAT"));
  					add(new TableFieldSchema().setName("score").setType("FLOAT"));

  					add(new TableFieldSchema().setName("wikipedia").setType("STRING"));
  					add(new TableFieldSchema().setName("mid").setType("STRING"));
  				}
  			}).setMode("REPEATED"));
  	return new TableSchema().setFields(fields);
  }
  
  /**
   * Run a batch or streaming pipeline.
   */
  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		options.setMaxNumWorkers(16);
		options.setWorkerMachineType("n1-standard-2");
		options.setZone("us-east1-d");
		
    Pipeline pipeline = Pipeline.create(options);
    
    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(options.as(Options.class).getOutputDataset());
    tableRef.setProjectId(options.as(GcpOptions.class).getProject());
    tableRef.setTableId(options.getOutputTableName());

    // Read events from either a BigQuery query or PubSub stream.
    pipeline
    .apply(new ReadComments(options))
    .apply("GetSentiment", ParDo.of(new GetSentimentFn()))
    .apply(
    	BigQueryIO.writeTableRows().to(tableRef)
    	.withSchema(getSchema())
    	.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
    	.withWriteDisposition(WriteDisposition.WRITE_APPEND))
    ;

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
  }
}