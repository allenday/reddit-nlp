package com.allenday;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.bigquery.model.TableCell;
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
        new TableFieldSchema().setName("entities").setType("RECORD").setFields(
          new ArrayList<TableFieldSchema>() {
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
	
	public static void main(String[] args) throws IOException {
    //Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    DataflowPipelineOptions pipeOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		pipeOptions.setJobName("redditEntitySentiment");
		pipeOptions.setTempLocation("gs://allenday-dev/tmp");
		pipeOptions.setMaxNumWorkers(20);
		pipeOptions.setWorkerMachineType("n1-highcpu-64");
		pipeOptions.setProject("allenday-dev");
		pipeOptions.setZone("us-east1-d");

    Pipeline pipeline = Pipeline.create(pipeOptions);
    
    TableReference oTableRef = new TableReference();
    oTableRef.setProjectId(pipeOptions.getProject());
    oTableRef.setDatasetId("reddit_nlp");
    oTableRef.setTableId("reddit_posts");
    
    String query =
    		"SELECT CONCAT(CAST(created_utc AS STRING), '.', subreddit, '.', author) AS id, title, selftext " +
    		"FROM `fh-bigquery.reddit_posts.20*`";

    pipeline
      .apply(
    		BigQueryIO.read().fromQuery(query).usingStandardSql()
      )
      .apply(
    		"GetSentiment",
    		ParDo.of(new DoFn<TableRow,TableRow>() { //List<Float>
    			@ProcessElement
    			public void processElement(ProcessContext c) {
    				List<TableCell> cells = c.element().getF();
    				String id   = (String)c.element().get("id");
    				String body =
    						(String)c.element().get("title") + "\n\n" + (String)c.element().get("selftext");
      			//LOG.info(id + ":" + body);
    				Document doc = Document.newBuilder()
    					.setContent(body)
    					.setLanguage("en")
    					.setType(Type.PLAIN_TEXT)
    					.build();
    				Float score = 999f;
    				Float magnitude = 999f;
    				
    				try {
    					LanguageServiceClient language = LanguageServiceClient.create();

    					Sentiment docSentiment = language.analyzeSentiment(doc).getDocumentSentiment();
    					
    					Float docScore = docSentiment.getScore();
    					Float docMagnitude = docSentiment.getMagnitude();
    					
    					AnalyzeEntitySentimentResponse aesRes = language.analyzeEntitySentiment(doc, EncodingType.UTF8);
    					List<Entity> entities = aesRes.getEntitiesList();
    					
	    		    TableRow row = new TableRow();
    		      row.set("id", id);
	    		    row.set("score", docScore);
	    		    row.set("magnitude", docMagnitude);

	    		    List<TableRow> entityRecords = new ArrayList<TableRow>();
    					    					
    					for (Entity entity : entities) {
    						TableRow eRow = new TableRow();
    						Sentiment entitySentiment = entity.getSentiment();			
    						Map<String,Float> stats = new ArrayMap<String,Float>();

    						eRow.set("name", entity.getName());	
    						eRow.set("salience", entity.getSalience());
    						eRow.set("magnitude", entitySentiment.getMagnitude());
    						eRow.set("score", entitySentiment.getScore());	
    						if ( entity.getMetadataMap().containsKey("wikipedia_url") ) {
    							eRow.set("wikipedia", entity.getMetadataMap().get("wikipedia_url"));
    						}
    						if ( entity.getMetadataMap().containsKey("mid") ) {
    							eRow.set("mid", entity.getMetadataMap().get("mid"));
    						}
    						entityRecords.add(eRow);
    					}
    					row.set("entities", entityRecords);	      					
    					c.output(row);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}    					
    		  }
    	  })
      )
      .apply(
        BigQueryIO.writeTableRows().to(oTableRef)
          .withSchema(getOutputSchema())
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      )
      ;
    PipelineResult result = pipeline.run();
    //result.waitUntilFinish();
	}	
}