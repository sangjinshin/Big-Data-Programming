package com.sangjinshin.cs378.assign6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.sessions.BodyStyle;
import com.refactorlabs.cs378.sessions.CabStyle;
import com.refactorlabs.cs378.sessions.Condition;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.utils.Utils;

/**
 * CS 378 Big Data Programming Assignment 6 - Reduce Side Join
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
 * SessionGenerator app for User Sessions
 */
public class SessionGenerator extends Configured implements Tool {

  /**
   * Map class for SessionGenerator
   */
  public static class SessionMapper extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] values = line.split("\\t"); // for general field parsing
      String[] events = values[1].split(" ", 2); // for parsing eventType and eventSubtype
      String[] features = values[values.length - 1].split(":"); // for parsing features field
      Arrays.sort(features);

      /**
       * String[] values:
       * 
       * [userID, event_type, event_timestamp, city, vin, vehicle_condition, year, make, model,
       * trim, body_style, cab_style, price, mileage, free_carfax_report, features]
       * 
       * String[] events:
       * 
       * [event_type, event_subtype]
       * 
       * String[] features:
       * 
       * [feature0, feature1, feature2, ...]
       */

      /**
       * Initialize parsed fields with appropriate values
       */
      String userId = values[0];

      EventType eventType = null;
      if (!events[0].equals("null")) {
        eventType = EventType.valueOf(events[0].toUpperCase());
      }

      EventSubtype eventSubtype = null;
      if (!events[1].equals("null")) {
        eventSubtype = EventSubtype.valueOf(events[1].replaceAll(" ", "_").toUpperCase());
      }

      String eventTime = values[2];

      String city = values[3];

      String vin = values[4];

      Condition condition = null;
      if (!values[5].equals("null")) {
        condition = Condition.valueOf(values[5]);
      }

      int year = Integer.parseInt(values[6]);

      String make = values[7];

      String model = values[8];

      String trim = values[9];

      BodyStyle bodyStyle = null;
      if (!values[10].equals("null")) {
        bodyStyle = BodyStyle.valueOf(values[10]);
      }

      CabStyle cabStyle = null;
      if (!values[11].equals("null")) {
        int endIndex = values[11].indexOf(" ");
        cabStyle = CabStyle.valueOf(values[11].substring(0, endIndex));
      }

      float price = Float.parseFloat(values[12]);

      int mileage = Integer.parseInt(values[13]);

      boolean free_carfax_report = Boolean.parseBoolean(values[14].replaceAll("t", "true"));

      List<CharSequence> featuresList = new ArrayList<CharSequence>(Arrays.asList(features));
      featuresList.remove("null");

      // Create a new Event object with corresponding args created above
      Event event = new Event(eventType, eventSubtype, eventTime, city, vin, condition, year, make,
          model, trim, bodyStyle, cabStyle, price, mileage, free_carfax_report, featuresList);

      List<Event> eventList = new ArrayList<Event>();
      eventList.add(event);

      // Store userId and list of Event objects in builder
      Session.Builder builder = Session.newBuilder();
      builder.setUserId(userId);
      builder.setEvents(eventList);

      // Write output to context
      word.set(userId);
      context.write(word, new AvroValue<Session>(builder.build()));
    }
  }

  /**
   * The Reduce class for SessionGenerator
   */
  public static class SessionReducer
      extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

    @Override
    public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
        throws IOException, InterruptedException {

      Session.Builder builder = Session.newBuilder();

      List<Event> eventList = new ArrayList<Event>();

      // Aggregate Event without duplicates
      for (AvroValue<Session> value : values) {
        Session session = value.datum();
        if (!eventList.contains(session.getEvents().get(0))) {
          eventList.add(session.getEvents().get(0));
        }
      }

      // Sort eventList
      Collections.sort(eventList, new eventComparator());

      // Store userId and new list of Event objects in builder
      builder.setUserId(key.toString());
      builder.setEvents(eventList);

      // output for the reducer class
      context.write(new AvroKey<CharSequence>(key.toString()),
          new AvroValue<Session>(builder.build()));
    }

    // Custom comparator for sorting Event
    // Sort first by event_timestamp then by event_type
    private static class eventComparator implements Comparator<Event> {
      public int compare(Event e1, Event e2) {
        String s1 = e1.getEventTime().toString();
        String s2 = e2.getEventTime().toString();
        if (!s1.equals(s2)) {
          return s1.compareTo(s2);
        } else {
          s1 = e1.getEventType().toString();
          s2 = e2.getEventType().toString();
          return s1.compareTo(s2);
        }
      }
    }

  }

  /**
   * The run() method is called (indirectly) from main(), and contains all the job setup and
   * configuration.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: SessionGenerator <input path> <output path>");
      return -1;
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "SessionGenerator");
    String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Identify the JAR file to replicate to all machines.
    job.setJarByClass(Session.class);

    // Specify the Map
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(SessionMapper.class);
    job.setMapOutputKeyClass(Text.class);
    AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

    // Specify the Reduce
    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    job.setReducerClass(SessionReducer.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Session.getClassSchema());

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
    int res = ToolRunner.run(new SessionGenerator(), args);
    System.exit(res);
  }

}
