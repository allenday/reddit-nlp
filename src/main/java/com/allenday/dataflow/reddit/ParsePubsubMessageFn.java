package com.allenday.dataflow.reddit;

import java.util.Map;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


public class ParsePubsubMessageFn extends DoFn<PubsubMessage, Comment> {
	private static final long serialVersionUID = -7568780863920312060L;
  private static final Logger LOG = LoggerFactory.getLogger(ParsePubsubMessageFn.class);
	private static final Gson gson = new Gson();

	@ProcessElement
	public void processElement(ProcessContext c) {
		String json = new String(c.element().getPayload());
    Map<String, String> msg = gson.fromJson(json, new TypeToken<Map<String, String>>(){}.getType());		
		//for (String key : msg.keySet()) {
		//	System.err.println("\tk="+key+",v="+msg.get(key));
		//}
		String id = msg.get("id");
		String title = msg.get("title");
		String selftext = msg.get("selftext");
		Comment comment = new Comment(id, title, selftext);
		c.output(comment);
	}
}
