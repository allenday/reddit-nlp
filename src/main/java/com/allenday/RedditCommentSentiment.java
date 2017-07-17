package com.allenday;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.language.v1beta2.AnalyzeEntitySentimentResponse;
import com.google.cloud.language.v1beta2.Document;
import com.google.cloud.language.v1beta2.Document.Type;
import com.google.cloud.language.v1beta2.EncodingType;
import com.google.cloud.language.v1beta2.Entity;
import com.google.cloud.language.v1beta2.LanguageServiceClient;
import com.google.cloud.language.v1beta2.Sentiment;

public class RedditCommentSentiment {
	private static final Logger LOG = LoggerFactory.getLogger(RedditCommentSentiment.class);
	
  public static TableSchema getOutputSchema() {
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
	
  //static WriteDisposition disposition = WriteDisposition.WRITE_APPEND;
  
	public static void main(String[] args) throws IOException {
    //Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    DataflowPipelineOptions pipeOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		pipeOptions.setJobName("redditEntitySentiment-"+(int)(Math.random() * 10000));
		pipeOptions.setTempLocation("gs://allenday-dev/tmp");
		pipeOptions.setMaxNumWorkers(12);
		pipeOptions.setWorkerMachineType("n1-highmem-64");
		pipeOptions.setProject("allenday-dev");
		pipeOptions.setZone("us-east1-d");
		pipeOptions.setRunner(DataflowRunner.class);
		pipeOptions.setDefaultWorkerLogLevel(Level.WARN);

    Pipeline pipeline = Pipeline.create(pipeOptions);
 
    TableReference oTableRef = new TableReference();
    oTableRef.setProjectId(pipeOptions.getProject());
    oTableRef.setDatasetId("reddit_nlp_posts");
    
    WriteDisposition disposition = WriteDisposition.WRITE_TRUNCATE;
    String tab = "2017_04";
    oTableRef.setTableId(tab);
    String query =
    		"SELECT id, title, selftext FROM `fh-bigquery.reddit_posts."+tab+"` " +
        "WHERE id NOT IN (SELECT id FROM `allenday-dev.reddit_nlp_posts."+tab+"`) " +
    		"LIMIT 100000";

    pipeline
      .apply(
    		BigQueryIO.read().fromQuery(query).usingStandardSql()
      )
      .apply(
    		"GetSentiment",
    		ParDo.of(new DoFn<TableRow,TableRow>() {
					private static final long serialVersionUID = 9202497069970159732L;
					Long sleep = 50L;
          Long sleepStep = 50L;

    			@ProcessElement
    			public void processElement(ProcessContext c) {
            
    				String id   = (String)c.element().get("id");
    				String body =
    						(String)c.element().get("title") + "\n\n" + (String)c.element().get("selftext");
      			//LOG.info(id + ":" + body);
    				Document doc = Document.newBuilder()
    					.setContent(body)
    					//.setLanguage("en")
    					.setType(Type.PLAIN_TEXT)
    					.build();
    				Float defaultScore = 9999f;
    				Float defaultMagnitude = 9999f;
    				
    				Boolean parsed = false;
    				for ( int t = 0; t < 40; t++ ) {
    					try {
    						LanguageServiceClient language = LanguageServiceClient.create();

    						Sentiment docSentiment = language.analyzeSentiment(doc).getDocumentSentiment();
    						Float docScore = docSentiment.getScore();
    						Float docMagnitude = docSentiment.getMagnitude();
    						
//    						AnalyzeEntitySentimentResponse aesRes = language.analyzeEntitySentiment(doc, EncodingType.UTF8);
//    						List<Entity> entities = aesRes.getEntitiesList();
//    						List<TableRow> entityRecords = new ArrayList<TableRow>();
//
//    						for (Entity entity : entities) {
//    							TableRow eRow = new TableRow();
//    							Sentiment entitySentiment = entity.getSentiment();			
//
//    							eRow.set("name", entity.getName());	
//    							eRow.set("salience", entity.getSalience());
//    							eRow.set("magnitude", entitySentiment.getMagnitude());
//    							eRow.set("score", entitySentiment.getScore());	
//    							if ( entity.getMetadataMap().containsKey("wikipedia_url") ) {
//    								eRow.set("wikipedia", entity.getMetadataMap().get("wikipedia_url"));
//    							}
//    							if ( entity.getMetadataMap().containsKey("mid") ) {
//    								eRow.set("mid", entity.getMetadataMap().get("mid"));
//    							}
//    							entityRecords.add(eRow);
//    						}
    						TableRow row = new TableRow();
    						row.set("id", id);
    						row.set("score", docScore);
    						row.set("magnitude", docMagnitude);
//  						row.set("entity", entityRecords);	      					
    						c.output(row);
    						parsed = true;
    						break;
    					} catch (Exception e) {
    						try {
    							LOG.warn("language API exception, sleeping 5s..\n\n"+StringUtils.join(e.getStackTrace(),"\n"));
    							Thread.sleep(5000);
    							//if (Math.random() < 0.1 && sleep < 1000L) {
    							//	LOG.warn("increasing inter item sleep to " + sleep + " ms");
    							//	sleep += sleepStep;
    							//}
    						} catch (InterruptedException e1) {
    							//LOG.error("failed to sleep:\n" + e1.getStackTrace());
    						}
    					}    					
    				}
    				if ( !parsed ) {
  						LOG.info("***"+id+"***");
//    					//after 200s of failures (40 attempts) capture ID  
//    					//and default score/magnitude to represent failure
//    					TableRow row = new TableRow();
//    					row.set("id", id);
//    					row.set("score", defaultScore);
//    					row.set("magnitude", defaultMagnitude);
//    					c.output(row);
    				}
    			}
    	  })
      )
      .apply(
        BigQueryIO.writeTableRows().to(oTableRef)
          .withSchema(getOutputSchema())
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//          .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
        .withWriteDisposition(disposition)
      )
      ;
    PipelineResult result = pipeline.run();
    //result.waitUntilFinish();
	}	
}