package com.allenday;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.api.client.util.ArrayMap;
import com.google.cloud.language.v1beta2.AnalyzeEntitySentimentResponse;
import com.google.cloud.language.v1beta2.Document;
import com.google.cloud.language.v1beta2.Document.Type;
import com.google.cloud.language.v1beta2.EncodingType;
import com.google.cloud.language.v1beta2.Entity;
import com.google.cloud.language.v1beta2.LanguageServiceClient;
import com.google.cloud.language.v1beta2.Sentiment;

public class LanguageTest {
	public static void main(String[] args) throws Exception {
		String comment = "I am 100% confident I have outperformed you but who cares, we are here to make money, not lose it. There is enough evidence that says BTC will go down. I suggest you take a step back and look at the big picture, there will be another leg down shortly in BTC";
		Document doc = Document.newBuilder()
				.setContent(comment)
				.setLanguage("en")
				.setType(Type.PLAIN_TEXT)
				.build();
		LanguageServiceClient language = LanguageServiceClient.create();
		Sentiment docSentiment = language.analyzeSentiment(doc).getDocumentSentiment();
		
		Float docScore = docSentiment.getScore();
		Float docMagnitude = docSentiment.getMagnitude();
		
		AnalyzeEntitySentimentResponse aesRes = language.analyzeEntitySentiment(doc, EncodingType.UTF8);
		List<Entity> entities = aesRes.getEntitiesList();

		Map<String,Map<String,Float>> entityStats = new ArrayMap<String,Map<String,Float>>();
		Map<String,Map<String,String>> entityMetas = new ArrayMap<String,Map<String,String>>();
		
		Map<String,Object> parse = new ArrayMap<String,Object>();
		parse.put("score", docScore);
		parse.put("magnitude", docMagnitude);
		
		for (Entity entity : entities) {
			Sentiment entitySentiment = entity.getSentiment();			
			Map<String,Float> stats = new ArrayMap<String,Float>();
			stats.put("salience", entity.getSalience());
			stats.put("magnitude", entitySentiment.getMagnitude());
			stats.put("score", entitySentiment.getScore());	

			entityStats.put(entity.getName(), stats);
			entityMetas.put(entity.getName(), entity.getMetadataMap());
			
			System.err.println(entity.getName() + " " + entity.getSalience() + " " + entitySentiment.getMagnitude() + " " + entitySentiment.getScore());
		}
		
		parse.put("entity_metadata", entityMetas);
		parse.put("entity_measures", entityStats);
    String mapAsJson = new ObjectMapper().writeValueAsString(parse);
    System.out.println(mapAsJson);

	}
}
