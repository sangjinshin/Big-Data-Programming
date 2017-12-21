package com.sangjinshin.cs378.assign3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * CS 378 Big Data Programming Assignment 3 - Inverted Index
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 * 
 */

public class EmailReference {

  /**
   * 
   * The mapper parses the email to output email addresses that are referenced in a particular
   * message id. The email headers are identified first then following email addresses are each
   * concatenated to the corresponding header.
   * 
   * The email address is output as the key and the message id is output as the value.
   *
   */
  public static class EmailReferenceMapper extends Mapper<Object, Text, Text, Text> {

    private Text email_address = new Text();
    private Text message_id = new Text();

    private final String MESSAGE_ID = "Message-ID:";
    private final String FROM = "From:";
    private final String TO = "To:";
    private final String CC = "Cc:";
    private final String BCC = "Bcc:";
    private final String X_FROM = "X-From:";

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      // Get an email in string
      String line = value.toString();
      // Remove all commas since they mess with
      // tokenizing just email address
      line = line.replaceAll("[,]", " ");

      StringTokenizer tokenizer = new StringTokenizer(line);
      String curr_token = "";
      String header = "";
      boolean continue_parsing = true;

      // Iterate over the tokens and determine which header
      // is currently being parsed. Then create full email_address
      // key by concatenating the header and the address.
      while (continue_parsing && tokenizer.hasMoreTokens()) {
        curr_token = tokenizer.nextToken();

        // Stop parsing when token reaches "X-From:"
        if (curr_token.equalsIgnoreCase(X_FROM)) {
          continue_parsing = false;
        } else if (curr_token.equalsIgnoreCase(MESSAGE_ID)) {
          curr_token = tokenizer.nextToken();
          message_id.set(curr_token); // Set id value
        } else if (curr_token.equalsIgnoreCase(FROM)) {
          header = FROM; // "From:"
        } else if (curr_token.equalsIgnoreCase(TO)) {
          header = TO; // "To:"
        } else if (curr_token.equalsIgnoreCase(CC)) {
          header = CC; // "Cc:"
        } else if (curr_token.equalsIgnoreCase(BCC)) {
          header = BCC; // "Bcc:"
        } else if (curr_token.contains("@")) { // address token
          // Set full header+address key
          email_address.set(header + curr_token);
          // Write out the resulting k/v pair
          context.write(email_address, message_id);
        } else {
          continue;
        }
      }
    }
  }

  /**
   * 
   * The reducer iterates through the set of input values and adds each string into a local
   * ArrayList to check for duplicates and finally sort. Then the input key is output as one string.
   *
   */
  public static class EmailReferenceReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      List<String> message_id_list = new ArrayList<String>();

      StringBuilder sb = new StringBuilder();

      // Add the mesesage_id values into ArrayList
      // checking for duplicates
      for (Text v : values) {
        String s = v.toString();
        if (!message_id_list.contains(s)) {
          message_id_list.add(v.toString());
        }
      }

      // Sort the message_id_list
      Collections.sort(message_id_list);

      // Append all message_id values into one string
      for (String s : message_id_list) {
        if (message_id_list.indexOf(s) == message_id_list.size() - 1) {
          sb.append(s);
        } else {
          sb.append(s + ",");
        }
      }

      result.set(sb.toString());
      context.write(key, result);
    }
  }

  /**
   * 
   * Specify characteristics of MapReduce job and initiate
   * 
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = Job.getInstance(conf, "emailreference");
    // Identify the JAR file to replicate to all machines.
    job.setJarByClass(EmailReference.class);

    // Set the output key and value types (for map and reduce).
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set the map and reduce classes.
    job.setMapperClass(EmailReferenceMapper.class);
    job.setReducerClass(EmailReferenceReducer.class);

    // Set the input and output file formats.
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Grab the input file and output directory from the command line.
    FileInputFormat.addInputPaths(job, appArgs[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Initiate the map-reduce job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
