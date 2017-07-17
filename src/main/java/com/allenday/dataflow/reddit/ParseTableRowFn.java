package com.allenday.dataflow.reddit;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableRow;

public class ParseTableRowFn extends DoFn<TableRow, Comment> {
	private static final long serialVersionUID = 361426707564723696L;

	public void processElement(ProcessContext c) {
		TableRow row = c.element();
		String id = (String)row.get("id");
		String title = (String)row.get("title");
		String selftext = (String)row.get("selftext");
		Comment comment = new Comment(id, title, selftext);
		c.output(comment);
	}
}
