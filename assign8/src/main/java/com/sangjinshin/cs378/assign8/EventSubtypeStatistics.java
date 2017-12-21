package com.sangjinshin.cs378.assign8;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.assign8.EventSubtypeStatisticsData;
import com.refactorlabs.cs378.assign8.EventSubtypeStatisticsKey;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.sessions.SessionType;

/**
 * CS 378 Big Data Programming Assignment 8 - Job Chaining
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 */

public class EventSubtypeStatistics extends Configured implements Tool {

  private static final long ONE = 1L;

  // SessionType enums
  public static final String SUBMITTER = SessionType.SUBMITTER.getText();
  public static final String CLICKER = SessionType.CLICKER.getText();
  public static final String SHOWER = SessionType.SHOWER.getText();
  public static final String VISITOR = SessionType.VISITOR.getText();
  public static final String OTHER = SessionType.OTHER.getText();

  public static abstract class EventSubtypeStatsMapper extends
      Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

    // Specify specific session type to calculate statistics on
    protected abstract String sessionType();

    @Override
    protected void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
        throws IOException, InterruptedException {

      // Builder for key and value output
      EventSubtypeStatisticsKey.Builder keyBuilder;
      EventSubtypeStatisticsData.Builder dataBuilder;

      // HashMap to keep record of # of EventSubtype in Session
      Map<EventSubtype, Long> eventSubtypeMap = new HashMap<EventSubtype, Long>();

      Session session = value.datum();
      List<Event> eventsList = session.getEvents();

      // Put # of EventSubtype into eventSubtypeMap
      for (Event e : eventsList) {
        EventSubtype es = e.getEventSubtype();
        if (!eventSubtypeMap.containsKey(es)) {
          eventSubtypeMap.put(es, ONE);
        } else {
          eventSubtypeMap.put(es, eventSubtypeMap.get(es) + ONE);
        }
      }

      // Set K,V for specific session type to write to Context
      // Iterate over EventSubtype.values() since some EventSubtypes might not be in the
      // eventSubtypeMap but still need to calculate statistics for them.
      for (EventSubtype es : EventSubtype.values()) {
        keyBuilder = EventSubtypeStatisticsKey.newBuilder();
        dataBuilder = EventSubtypeStatisticsData.newBuilder();

        // Set the key
        keyBuilder.setSessionType(sessionType());
        keyBuilder.setEventSubtype(es.toString());

        // Set the value
        long totalCount = 0;
        Long count = eventSubtypeMap.get(es);
        if (count != null) {
          totalCount = count.longValue();
        }
        dataBuilder.setSessionCount(ONE);
        dataBuilder.setTotalCount(totalCount);
        dataBuilder.setSumOfSquares(totalCount * totalCount);

        context.write(new AvroKey<EventSubtypeStatisticsKey>(keyBuilder.build()),
            new AvroValue<EventSubtypeStatisticsData>(dataBuilder.build()));
      }

      // Set K,V for all session type to write to Context
      keyBuilder = EventSubtypeStatisticsKey.newBuilder();
      dataBuilder = EventSubtypeStatisticsData.newBuilder();
      long totalCount = 0;

      for (EventSubtype es : EventSubtype.values()) { // Calculate totalCount for all session type
        Long count = eventSubtypeMap.get(es);
        if (count != null) {
          totalCount += count.longValue();
        }
      }

      // Set the key
      keyBuilder.setSessionType(sessionType());
      keyBuilder.setEventSubtype("any");

      // Set the value
      dataBuilder.setSessionCount(ONE);
      dataBuilder.setTotalCount(totalCount);
      dataBuilder.setSumOfSquares(totalCount * totalCount);

      context.write(new AvroKey<EventSubtypeStatisticsKey>(keyBuilder.build()),
          new AvroValue<EventSubtypeStatisticsData>(dataBuilder.build()));
    }
  }

  public static class SubmitterMapper extends EventSubtypeStatsMapper {
    @Override
    protected String sessionType() {
      return SUBMITTER;
    }
  }

  public static class ClickerMapper extends EventSubtypeStatsMapper {
    @Override
    protected String sessionType() {
      return CLICKER;
    }
  }

  public static class ShowerMapper extends EventSubtypeStatsMapper {
    @Override
    protected String sessionType() {
      return SHOWER;
    }
  }

  public static class VisitorMapper extends EventSubtypeStatsMapper {
    @Override
    protected String sessionType() {
      return VISITOR;
    }
  }

  /**
   * The Reducer class for EventSubtypeStatistics
   * 
   * Mean and variance are calculated using respective formula as follows:
   * 
   * mean = totalCount / sessionCount
   * 
   * variance = mean of squares - square of means<br>
   * = (sumOfSquares / sessionCount) - (mean * mean)
   *
   */
  public static class EventSubtypeStatsReducer extends
      Reducer<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

    @Override
    public void reduce(AvroKey<EventSubtypeStatisticsKey> key,
        Iterable<AvroValue<EventSubtypeStatisticsData>> values, Context context)
        throws IOException, InterruptedException {
      long sessionCount = 0;
      long totalCount = 0;
      long sumOfSquares = 0;
      double mean = 0.0;
      double variance = 0.0;

      // Aggregate count values from Mapper
      for (AvroValue<EventSubtypeStatisticsData> value : values) {
        sessionCount += value.datum().getSessionCount();
        totalCount += value.datum().getTotalCount();
        sumOfSquares += value.datum().getSumOfSquares();
      }

      // Calculate mean and variance
      mean = (double) totalCount / sessionCount;
      variance = ((double) sumOfSquares / sessionCount) - (mean * mean);

      // Builder for value output
      EventSubtypeStatisticsData.Builder dataBuilder = EventSubtypeStatisticsData.newBuilder();
      dataBuilder.setSessionCount(sessionCount);
      dataBuilder.setTotalCount(totalCount);
      dataBuilder.setSumOfSquares(sumOfSquares);
      dataBuilder.setMean(mean);
      dataBuilder.setVariance(variance);

      // Write output to Context
      context.write(key, new AvroValue<EventSubtypeStatisticsData>(dataBuilder.build()));
    }
  }

  /**
   * The run() method is called (indirectly) from main(), and contains all the job setup and
   * configuration.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: EventSubtypeStatistics <input path> <output path>");
      return -1;
    }

    Configuration conf = getConf();

    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    // Binning User Sessions Job
    Job binningJob = Job.getInstance(conf, "UserSessionFilter");
    String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    binningJob.setJarByClass(EventSubtypeStatistics.class);

    // Map-only job, so set the number of reducers to zero.
    binningJob.setNumReduceTasks(0);

    // Count the number of each session type written out
    MultipleOutputs.setCountersEnabled(binningJob, true);

    // Specify the Map
    binningJob.setMapperClass(UserSessionsFilter.UserSessionsFilterMapper.class);
    binningJob.setInputFormatClass(AvroKeyValueInputFormat.class);
    AvroJob.setInputKeySchema(binningJob, Schema.create(Schema.Type.STRING));
    AvroJob.setInputValueSchema(binningJob, Session.getClassSchema());
    binningJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    AvroJob.setOutputKeySchema(binningJob, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(binningJob, Session.getClassSchema());

    // Specify output category
    for (SessionType sessionType : SessionType.values()) {
      AvroMultipleOutputs.addNamedOutput(binningJob, sessionType.getText(),
          AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING),
          Session.getClassSchema());
    }

    FileInputFormat.addInputPath(binningJob, new Path(appArgs[0]));
    FileOutputFormat.setOutputPath(binningJob, new Path(appArgs[1]));

    // Initiate the binning job, and wait for completion.
    binningJob.waitForCompletion(true);

    // Input/Output Paths
    Path submitterInputDir = setInputDir(appArgs[1], SUBMITTER + "-m-00000.avro");
    Path submitterOutputDir = setOutputDir(appArgs[1], SUBMITTER);
    Path clickerInputDir = setInputDir(appArgs[1], CLICKER + "-m-00000.avro");
    Path clickerOutputDir = setOutputDir(appArgs[1], CLICKER);
    Path showerInputDir = setInputDir(appArgs[1], SHOWER + "-m-00000.avro");
    Path showerOutputDir = setOutputDir(appArgs[1], SHOWER);
    Path visitorInputDir = setInputDir(appArgs[1], VISITOR + "-m-00000.avro");
    Path visitorOutputDir = setOutputDir(appArgs[1], VISITOR);

    // Session Type Jobs
    Job submitterJob = submitJob("Submitter Job", conf, SubmitterMapper.class,
        new Path[] {submitterInputDir}, submitterOutputDir);
    Job clickerJob = submitJob("Clicker Job", conf, ClickerMapper.class,
        new Path[] {clickerInputDir}, clickerOutputDir);
    Job showerJob = submitJob("Shower Job", conf, ShowerMapper.class, new Path[] {showerInputDir},
        showerOutputDir);
    Job visitorJob = submitJob("Visitor Job", conf, VisitorMapper.class,
        new Path[] {visitorInputDir}, visitorOutputDir);

    // While jobs are not finished, sleep
    while (!submitterJob.isComplete() || !clickerJob.isComplete() || !showerJob.isComplete()
        || !visitorJob.isComplete()) {
      Thread.sleep(5000);
    }

    // Run Aggregate Job when all previous jobs are successfully complete
    if (submitterJob.isSuccessful() && clickerJob.isSuccessful() && showerJob.isSuccessful()
        && visitorJob.isSuccessful()) {
      Path[] inputPaths = {setInputDir(appArgs[1], SUBMITTER, "part-r-00000.avro"),
          setInputDir(appArgs[1], CLICKER, "part-r-00000.avro"),
          setInputDir(appArgs[1], SHOWER, "part-r-00000.avro"),
          setInputDir(appArgs[1], VISITOR, "part-r-00000.avro")};

      Job aggregateJob = Job.getInstance(conf, "Aggregate Job");
      aggregateJob.setJarByClass(EventSubtypeStatistics.class);

      // Specify the Map
      aggregateJob.setMapperClass(Mapper.class);
      aggregateJob.setInputFormatClass(AvroKeyValueInputFormat.class);
      AvroJob.setInputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
      AvroJob.setInputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());
      AvroJob.setMapOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
      AvroJob.setMapOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());

      // Specify the Combiner
      aggregateJob.setCombinerClass(EventSubtypeStatsReducer.class);

      // Specify the Reduce
      aggregateJob.setReducerClass(EventSubtypeStatsReducer.class);
      aggregateJob.setOutputFormatClass(TextOutputFormat.class);
      AvroJob.setOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
      AvroJob.setOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());

      FileInputFormat.setInputPaths(aggregateJob, inputPaths);
      FileOutputFormat.setOutputPath(aggregateJob, setOutputDir(appArgs[1], "aggregate"));

      // Initiate the aggregate job, and wait for completion.
      aggregateJob.waitForCompletion(true);

      return 0;
    }

    return 1; // Should not reach here.
  }

  // Helper method for setting input path for session type job
  private static Path setInputDir(String baseInputPath, String bin) {
    return new Path(baseInputPath + "/" + bin);
  }

  // Helper method for setting input path for aggregate job
  private static Path setInputDir(String baseInputPath, String job, String fileName) {
    return new Path(baseInputPath + "/" + job + "Statistics" + "/" + fileName);
  }

  // Helper method for setting output path
  private static Path setOutputDir(String inputPath, String job) {
    return new Path(inputPath + "/" + job + "Statistics");
  }

  // Helper method for configuring job
  @SuppressWarnings("rawtypes")
  private static Job submitJob(String jobName, Configuration conf, Class<? extends Mapper> mapClass,
      Path[] inputDir, Path outputDir) throws Exception {
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(EventSubtypeStatistics.class);

    // Specify the Map
    job.setMapperClass(mapClass);
    job.setInputFormatClass(AvroKeyValueInputFormat.class);
    AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setInputValueSchema(job, Session.getClassSchema());
    AvroJob.setMapOutputKeySchema(job, EventSubtypeStatisticsKey.getClassSchema());
    AvroJob.setMapOutputValueSchema(job, EventSubtypeStatisticsData.getClassSchema());

    // Specify the Combiner
    job.setCombinerClass(EventSubtypeStatsReducer.class);
    job.setNumReduceTasks(1);

    // Specify the Reduce
    job.setReducerClass(EventSubtypeStatsReducer.class);
    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    AvroJob.setOutputKeySchema(job, EventSubtypeStatisticsKey.getClassSchema());
    AvroJob.setOutputValueSchema(job, EventSubtypeStatisticsData.getClassSchema());

    FileInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.submit();
    return job;
  }

  /**
   * The main method specifies the characteristics of the map-reduce job by setting values on the
   * Job object, and then initiates the map-reduce job and waits for it to complete.
   */
  public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new EventSubtypeStatistics(), args);
    System.exit(res);
  }
}
