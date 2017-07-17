/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.allenday.dataflow.reddit;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

class Injector {

	private static Pubsub pubsub;
	private static String topic;
	private static String project;
	private static final String MESSAGE_ID_ATTRIBUTE = "id";
	private static final int numMessages = 10;
	private static final int delayInMillis = 500;

	// How long to sleep, in ms, between creation of the threads that make API requests to PubSub.
	private static final int THREAD_SLEEP_MS = 500;

	private static final Gson gson = new Gson();

	public static void publishData(int numMessages, int delayInMillis) throws IOException {
		List<PubsubMessage> pubsubMessages = new ArrayList<>();

		for (int i = 0; i < Math.max(1, numMessages); i++) {
			Long currTime = System.currentTimeMillis();
			String id = UUID.randomUUID().toString();
			String msg = "msg_"+id;
			String title = "title_"+id;
			String selftext = "selftext happy "+id;
			ImmutableMap<String,String> payload = ImmutableMap.of(
					MESSAGE_ID_ATTRIBUTE, id,
					"title", title,
					"selftext", selftext
					);
      String json = gson.toJson(payload); 
			PubsubMessage pubsubMessage = new PubsubMessage().encodeData(json.getBytes("UTF-8"));
			pubsubMessage.setAttributes(payload);
			
      System.out.println(json);
			pubsubMessages.add(pubsubMessage);
		}

		PublishRequest publishRequest = new PublishRequest();
		publishRequest.setMessages(pubsubMessages);
		pubsub.projects().topics().publish(topic, publishRequest).execute();
	}

	public static void main(String[] args)
			throws GeneralSecurityException, IOException, InterruptedException {
		if (args.length < 2) {
			System.out.println(
					"Usage: Injector project-name (topic-name|none) ");
			System.exit(1);
		}
		project = args[0];
		String topicName = args[1];
		// Create the PubSub client.
		pubsub = InjectorUtils.getClient();
		// Create the PubSub topic as necessary.
		topic = InjectorUtils.getFullyQualifiedTopicName(project, topicName);
		InjectorUtils.createTopic(pubsub, topic);
		System.out.println("Injecting to topic: " + topic);
		System.out.println("Starting Injector");

		// Publish messages at a rate determined by the QPS and Thread sleep settings.
		for (int i = 0; true; i++) {
			// Start a thread to inject some data.
			new Thread() {
				public void run() {
					try {
						publishData(numMessages, delayInMillis);
					} catch (IOException e) {
						System.err.println(e);
					}
				}
			}.start();

			// Wait before creating another injector thread.
			Thread.sleep(THREAD_SLEEP_MS);
		}
	}
}
