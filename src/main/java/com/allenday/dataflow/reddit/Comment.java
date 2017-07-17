package com.allenday.dataflow.reddit;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Comment {
  String id;
  @Nullable
  String title;
  @Nullable
  String selftext;

  public Comment() {
  }
  
  public Comment(String id, String title, String selftext) {
    this.id = id;
    this.title = title;
    this.selftext = selftext;
  }

  public String getId() {
    return this.id;
  }

  public String getTitle() {
    return this.title;
  }

  public String getSelftext() {
    return this.selftext;
  }
}
