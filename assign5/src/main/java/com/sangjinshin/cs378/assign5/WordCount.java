package com.sangjinshin.cs378.assign5;

import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.utils.Utils;

/**
 * CS 378 Big Data Programming Assignment 5 - User Sessions
 * 
 * 
 * @author David Franke (dfranke@cs.utexas.edu)
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 * 
 */

/**
 * Modified WordCount app for User Sessions that shows the values that occur in each field
 */
public class WordCount extends Configured implements Tool {

  /**
   * Map class for modified WordCount
   */
  public static class WordCountMapper
      extends Mapper<LongWritable, Text, Text, AvroValue<WordCountData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    // Log entry headers
    private final String headers[] = new String[] {"user_id", "event_type", "event_timestamp",
        "city", "vin", "vehicle_condition", "year", "make", "model", "trim", "body_style",
        "cab_style", "price", "mileage", "free_carfax_report", "features"};

    // Headers to skip over
    private final String ignore[] =
        new String[] {"event_timestamp", "mileage", "price", "user_id", "vin"};

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] values = line.split("\\t");
      String[] features = values[values.length - 1].split(":"); // for parsing features field

      WordCountData.Builder builder = WordCountData.newBuilder();

      // Count desired header field values
      for (int i = 0; i < values.length - 1; i++) {
        String currentHeader = headers[i];
        String currentValue = values[i];
        if (!Arrays.asList(ignore).contains(currentHeader)) {
          word.set(currentHeader + ":" + currentValue);
          builder.setCount(Utils.ONE);
          context.write(word, new AvroValue<WordCountData>(builder.build()));
        }
      }

      // Count features field values
      for (int i = 0; i < features.length; i++) {
        String currentValue = features[i];
        word.set(headers[headers.length - 1] + ":" + currentValue);
        builder = WordCountData.newBuilder();
        builder.setCount(Utils.ONE);
        context.write(word, new AvroValue<WordCountData>(builder.build()));
      }
    }
  }

  /**
   * The Reduce class for modified WordCount
   */
  public static class WordCountReducer extends
      Reducer<Text, AvroValue<WordCountData>, AvroKey<CharSequence>, AvroValue<WordCountData>> {

    @Override
    public void reduce(Text key, Iterable<AvroValue<WordCountData>> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0L;

      // Sum up the counts for the current word, specified in object "key".
      for (AvroValue<WordCountData> value : values) {
        sum += value.datum().getCount();
      }

      // Emit the total count for the word.
      WordCountData.Builder builder = WordCountData.newBuilder();
      builder.setCount(sum);
      context.write(new AvroKey<CharSequence>(key.toString()),
          new AvroValue<WordCountData>(builder.build()));
    }

  }

  /**
   * The run() method is called (indirectly) from main(), and contains all the job setup and
   * configuration.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input path> <output path>");
      return -1;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "WordCount");
    String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Identify the JAR file to replicate to all machines.
    job.setJarByClass(WordCountData.class);

    // Specify the Map
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    AvroJob.setMapOutputValueSchema(job, WordCountData.getClassSchema());

    // Specify the Reduce
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setReducerClass(WordCountReducer.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, WordCountData.getClassSchema());

    // Grab the input file and output directory from the command line.
    FileInputFormat.addInputPaths(job, appArgs[0]);
    FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

    // Initiate the map-reduce job, and wait for completion.
    job.waitForCompletion(true);

    return 0;
  }

  /**
   * The main method specifies the characteristics of the map-reduce job by setting values on the
   * Job object, and then initiates the map-reduce job and waits for it to complete.
   */
  public static void main(String[] args) throws Exception {
    Utils.printClassPath();
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

}
