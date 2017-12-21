package com.sangjinshin.cs378.assign6;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.assign6.VinImpressionCounts;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.utils.Utils;

/**
 * CS 378 Big Data Programming Assignment 6 - Reduce Side Join
 * 
 * Performs left outer join with VIN as the foreign key on session data (left side) and VIN
 * impression data (right side)
 * 
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 */

public class ReduceSideJoin extends Configured implements Tool {

  /**
   * Map class for reading in AVRO container file from Sessions
   */
  public static class SessionJoinMapper extends
      Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

    private static final long ONE = 1L;
    private Text outkey = new Text();

    @Override
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
        throws IOException, InterruptedException {
      Set<String> uniqueUsersSet = new HashSet<String>();
      Map<String, HashMap<CharSequence, Long>> clicksMap =
          new HashMap<String, HashMap<CharSequence, Long>>();
      Set<String> editContactFormSet = new HashSet<String>();

      Session session = value.datum();

      for (Event event : session.getEvents()) {
        String vin = event.getVin().toString();
        EventType eventType = event.getEventType();
        EventSubtype eventSubtype = event.getEventSubtype();

        uniqueUsersSet.add(vin);
        if (eventType == EventType.CLICK) {
          if (!clicksMap.containsKey(vin)) {
            HashMap<CharSequence, Long> click = new HashMap<CharSequence, Long>();
            click.put(eventSubtype.toString(), ONE);
            clicksMap.put(vin, click);
          } else {
            HashMap<CharSequence, Long> click = clicksMap.get(vin);
            click.put(eventSubtype.toString(), ONE);
            clicksMap.put(vin, click);
          }
        } else if (eventType == EventType.EDIT && eventSubtype == EventSubtype.CONTACT_FORM) {
          editContactFormSet.add(vin);
        }
      }

      for (String vin : uniqueUsersSet) {
        VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
        builder.setUniqueUsers(ONE);
        if (clicksMap.containsKey(vin)) {
          builder.setClicks(clicksMap.get(vin));
        }
        if (editContactFormSet.contains(vin)) {
          builder.setEditContactForm(ONE);
        }
        outkey.set(vin);
        context.write(outkey, new AvroValue<VinImpressionCounts>(builder.build()));
      }

    }
  }

  /**
   * Map class for reading in CSV file
   */
  public static class VinImpressionJoinMapper
      extends Mapper<Object, Text, Text, AvroValue<VinImpressionCounts>> {

    private Text outkey = new Text();

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] values = line.split(",");

      /**
       * String[] values:
       * 
       * [VIN, Impression Type, Count]
       */

      String vin = values[0];
      String impressionType = values[1];
      long count = Long.parseLong(values[2]);

      VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
      if (impressionType.equals("SRP")) {
        builder.setMarketplaceSrps(count);
      } else if (impressionType.equals("VDP")) {
        builder.setMarketplaceVdps(count);
      }

      // The foreign join key is the VIN number
      outkey.set(vin);

      // Write output to context
      context.write(outkey, new AvroValue<VinImpressionCounts>(builder.build()));
    }
  }

  /**
   * Combiner class
   */
  public static class VinImpressionJoinCombiner
      extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

    public static VinImpressionCounts vinImpressionJoinCombiner(
        Iterable<AvroValue<VinImpressionCounts>> values) throws IOException {
      return VinImpressionJoinReducer.leftOuterJoin(values);
    }
  }

  /**
   * Reduce class
   */
  public static class VinImpressionJoinReducer
      extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

    @Override
    public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
        throws IOException, InterruptedException {
      VinImpressionCounts vinImpressionCounts = leftOuterJoin(values);
      if (vinImpressionCounts.getUniqueUsers() > 0) {
        context.write(key, new AvroValue<VinImpressionCounts>(vinImpressionCounts));
      }
    }

    public static VinImpressionCounts leftOuterJoin(Iterable<AvroValue<VinImpressionCounts>> values)
        throws IOException {

      Map<CharSequence, Long> totalClick = new HashMap<CharSequence, Long>();
      VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();

      for (AvroValue<VinImpressionCounts> v : values) {
        VinImpressionCounts vic = v.datum();
        Map<CharSequence, Long> click = vic.getClicks();
        if (click != null) {
          for (Map.Entry<CharSequence, Long> entry : click.entrySet()) {
            CharSequence eventSubtype = entry.getKey();
            if (!totalClick.containsKey(eventSubtype)) {
              totalClick.put(eventSubtype, click.get(eventSubtype));
            } else {
              totalClick.put(eventSubtype, totalClick.get(eventSubtype) + click.get(eventSubtype));
            }
          }
        }
        builder.setUniqueUsers(builder.getUniqueUsers() + vic.getUniqueUsers());
        builder.setEditContactForm(builder.getEditContactForm() + vic.getEditContactForm());
        builder.setMarketplaceSrps(builder.getMarketplaceSrps() + vic.getMarketplaceSrps());
        builder.setMarketplaceVdps(builder.getMarketplaceVdps() + vic.getMarketplaceVdps());
      }

      builder.setClicks(totalClick);

      return builder.build();

    }

  }

  /**
   * The run() method is called (indirectly) from main(), and contains all the job setup and
   * configuration.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: ReduceSideJoin <input path 1> <input path 2> <output path>");
      return -1;
    }

    Configuration conf = getConf();

    Job job = Job.getInstance(conf, "ReduceSideJoin");
    String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Identify the JAR file to replicate to all machines.
    job.setJarByClass(ReduceSideJoin.class);

    // Get multiple input files from the command line.
    MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class,
        SessionJoinMapper.class);
    MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class,
        VinImpressionJoinMapper.class);
    MultipleInputs.addInputPath(job, new Path(appArgs[2]), TextInputFormat.class,
        VinImpressionJoinMapper.class);

    // Specify the Avro input schema
    AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setInputValueSchema(job, Session.getClassSchema());

    // Specify the Map
    job.setMapOutputKeyClass(Text.class);
    AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

    // Set combiner class
    job.setCombinerClass(VinImpressionJoinCombiner.class);

    // Specify the Reduce
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setReducerClass(VinImpressionJoinReducer.class);
    job.setOutputKeyClass(Text.class);
    AvroJob.setOutputValueSchema(job, Session.getClassSchema());

    // Get the output directory from the command line.
    FileOutputFormat.setOutputPath(job, new Path(appArgs[3]));

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
    int res = ToolRunner.run(new ReduceSideJoin(), args);
    System.exit(res);
  }

}
