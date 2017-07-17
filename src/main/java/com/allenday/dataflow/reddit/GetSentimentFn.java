package com.allenday.dataflow.reddit;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.language.v1beta2.AnalyzeEntitySentimentResponse;
import com.google.cloud.language.v1beta2.Document;
import com.google.cloud.language.v1beta2.Document.Type;
import com.google.cloud.language.v1beta2.EncodingType;
import com.google.cloud.language.v1beta2.Entity;
import com.google.cloud.language.v1beta2.LanguageServiceClient;
import com.google.cloud.language.v1beta2.Sentiment;

public class GetSentimentFn extends DoFn<Comment, TableRow>{
	private static final long serialVersionUID = -8554638089010898859L;
	private static final Logger LOG = LoggerFactory.getLogger(GetSentimentFn.class);

	@ProcessElement
	public void processElement(ProcessContext c) {
		Comment comment = c.element();
		String body = comment.getTitle() + "\n\n" + comment.getSelftext();
		Document doc = Document.newBuilder()
			.setContent(body)
			.setLanguage("en")
			.setType(Type.PLAIN_TEXT)
			.build();

		Boolean parsed = false;
//		for ( int t = 0; t < 20; t++ ) {
			try {
				LanguageServiceClient language = LanguageServiceClient.create();

				Sentiment docSentiment = language.analyzeSentiment(doc).getDocumentSentiment();
				Float docScore = docSentiment.getScore();
				Float docMagnitude = docSentiment.getMagnitude();

				AnalyzeEntitySentimentResponse aesRes = language.analyzeEntitySentiment(doc, EncodingType.UTF8);
				List<Entity> entities = aesRes.getEntitiesList();
				List<TableRow> entityRecords = new ArrayList<TableRow>();

				for (Entity entity : entities) {
					TableRow eRow = new TableRow();
					Sentiment entitySentiment = entity.getSentiment();			

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
				
				TableRow row = new TableRow();
				row.set("id", comment.getId());
				row.set("score", docScore);
				row.set("magnitude", docMagnitude);
				row.set("entity", entityRecords);	      					
				c.output(row);
				parsed = true;
//				break;
			} catch (Exception e) {
				try {
					String st = StringUtils.join(e.getStackTrace(),"\n");
					if ( st.contains("is not supported") ) {
						LOG.warn("unsupported language, skipping");
//						break;
					}
					LOG.warn("language API exception, sleeping 0ms..\n\n");//+StringUtils.join(e.getStackTrace(),"\n"));
					//e.printStackTrace();
					Thread.sleep(0);
					//if (Math.random() < 0.1 && sleep < 1000L) {
					//	LOG.warn("increasing inter item sleep to " + sleep + " ms");
					//	sleep += sleepStep;
					//}
				} catch (InterruptedException e1) {
					//LOG.error("failed to sleep:\n" + e1.getStackTrace());
				}
			}    					
//		}
		if ( !parsed ) {
			LOG.info("*** failed to parse id="+comment.getId()+" ***");

		}	
	}
}
