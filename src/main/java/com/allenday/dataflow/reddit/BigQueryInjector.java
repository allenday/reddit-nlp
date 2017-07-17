package com.allenday.dataflow.reddit;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.Pubsub.Projects;
import com.google.api.services.pubsub.Pubsub.Projects.Topics;
import com.google.api.services.pubsub.Pubsub.Projects.Topics.Publish;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

public class BigQueryInjector {
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryInjector.class);
	private static final Gson gson = new Gson();

	public static void main(String[] args) throws IOException {
    //Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    DataflowPipelineOptions pipeOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		pipeOptions.setJobName("BigQueryInjector-"+(int)(Math.random() * 10000));
		pipeOptions.setTempLocation("gs://allenday-dev/tmp");
		pipeOptions.setMaxNumWorkers(3);
//		pipeOptions.setWorkerMachineType("n1-highmem-64");
		pipeOptions.setProject("allenday-dev");
		pipeOptions.setZone("us-east1-d");
		pipeOptions.setRunner(DataflowRunner.class);
		pipeOptions.setDefaultWorkerLogLevel(Level.WARN);


    Pipeline pipeline = Pipeline.create(pipeOptions);
 
    String query =
    		"SELECT id, title, selftext FROM `fh-bigquery.reddit_posts.2017*` " +
        "WHERE id NOT IN (SELECT id FROM `allenday-dev.reddit_nlp_posts.denorm_tmp_stream_out`) " +
    		//"ORDER BY created_utc DESC " +
    		"LIMIT 1000000";

    pipeline
      .apply(
      	"QueryBigQuery",
    		BigQueryIO.read().fromQuery(query).usingStandardSql()
      )
      .apply(
    		"PublishMessage",
    		ParDo.of(new DoFn<TableRow,Boolean>(){
					private static final long serialVersionUID = -6515475699901424019L;
					private Pubsub pubsub;
					
					@ProcessElement
    			public void processElement(ProcessContext c) {
    				String id       = (String)c.element().get("id");
    				String title    = (String)c.element().get("title");
    				String selftext = (String)c.element().get("selftext");
    				ImmutableMap<String,String> payload = ImmutableMap.of(
    						"id", id,
    						"title", title,
    						"selftext", selftext
    						);
    	      String json = gson.toJson(payload); 
    				PubsubMessage pubsubMessage;
    				List<PubsubMessage> msgs = new ArrayList<PubsubMessage>();
						try {
							pubsubMessage = new PubsubMessage().encodeData(json.getBytes("UTF-8"));
							msgs.add(pubsubMessage);
							PublishRequest publishRequest = new PublishRequest();
							publishRequest.setMessages(msgs);
							pubsub = InjectorUtils.getClient();
							Projects projects = pubsub.projects();
							Topics topics = projects.topics();
							Publish publish = topics.publish("projects/allenday-dev/topics/reddit_nlp_posts", publishRequest);
							publish.execute();
							c.output(true);
							Thread.sleep(0);
						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
    			}
    	  })
      )
      ;
    PipelineResult result = pipeline.run();
    //result.waitUntilFinish();
	}	
}