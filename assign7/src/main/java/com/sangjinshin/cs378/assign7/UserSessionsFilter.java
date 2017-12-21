package com.sangjinshin.cs378.assign7;

import java.io.IOException;
import java.util.Random;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.sessions.SessionType;

public class UserSessionsFilter extends Configured implements Tool {

  // Counters
  public static final String WRITE_COUNTER = "write counter";
  public static final String DISCARD_COUNTER = "discard counter";

  // SessionType enums
  public static final String SUBMITTER = SessionType.SUBMITTER.getText();
  public static final String CLICKER = SessionType.CLICKER.getText();
  public static final String SHOWER = SessionType.SHOWER.getText();
  public static final String VISITOR = SessionType.VISITOR.getText();
  public static final String OTHER = SessionType.OTHER.getText();

  public static class UserSessionsFilterMapper extends
      Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

    private AvroMultipleOutputs multipleOutputs;
    private Random rands = new Random();

    // Random sample threshold
    private final Double clickerPercentage = 0.1; // 1 in 10
    private final Double showerPercentage = 0.02; // 1 in 50

    @Override
    public void setup(Context context) {
      multipleOutputs = new AvroMultipleOutputs(context);
    }

    @Override
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
        throws IOException, InterruptedException {
      Session session = value.datum();

      boolean isSubmitter = false;
      boolean isClicker = false;
      boolean isShower = false;
      boolean isVisitor = false;
      boolean isOther = false;

      if (session.getEvents().size() > 100) { // filter out session with more than 100 events
        context.getCounter(DISCARD_COUNTER, "Large Session Discarded").increment(1L);
      } else {
        for (Event e : session.getEvents()) {
          EventType eventType = e.getEventType();
          EventSubtype eventSubtype = e.getEventSubtype();
          if ((eventType == EventType.CHANGE || eventType == EventType.EDIT
              || eventType == EventType.SUBMIT) && eventSubtype == EventSubtype.CONTACT_FORM) {
            isSubmitter = true;
          } else if (eventType == EventType.CLICK) {
            isClicker = true;
          } else if ((eventType == EventType.SHOW) || (eventType == EventType.DISPLAY)) {
            isShower = true;
          } else if (eventType == EventType.VISIT) {
            isVisitor = true;
          } else {
            isOther = true;
          }
        }

        if (isSubmitter) {
          context.getCounter(WRITE_COUNTER, SUBMITTER).increment(1L);
          multipleOutputs.write(SUBMITTER, key, value);
        }
        if (isClicker) {
          if (rands.nextDouble() < clickerPercentage) { // random sample 1 in 10
            context.getCounter(WRITE_COUNTER, CLICKER).increment(1L);
            multipleOutputs.write(CLICKER, key, value);
          } else {
            context.getCounter(DISCARD_COUNTER, "Clicker Session Discarded").increment(1L);
          }
        }
        if (isShower) {
          if (rands.nextDouble() < showerPercentage) { // random sample 1 in 50
            context.getCounter(WRITE_COUNTER, SHOWER).increment(1L);
            multipleOutputs.write(SHOWER, key, value);
          } else {
            context.getCounter(DISCARD_COUNTER, "Shower Session Discarded").increment(1L);
          }
        }
        if (isVisitor) {
          context.getCounter(WRITE_COUNTER, VISITOR).increment(1L);
          multipleOutputs.write(VISITOR, key, value);
        }
        if (isOther) {
          context.getCounter(WRITE_COUNTER, OTHER).increment(1L);
          multipleOutputs.write(OTHER, key, value);
        }
      }
    }

    @Override
    public void cleanup(Context context) throws InterruptedException, IOException {
      multipleOutputs.close();
    }

  }

  /**
   * The run() method is called (indirectly) from main(), and contains all the job setup and
   * configuration.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: UserSessionsFilter <input path> <output path>");
      return -1;
    }

    Configuration conf = getConf();

    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    Job job = Job.getInstance(conf, "UserSessionsFilter");
    String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Count the number of each session type written out
    AvroMultipleOutputs.setCountersEnabled(job, true);

    // Map-only job, so set the number of reducers to zero.
    job.setNumReduceTasks(0);

    // Identify the JAR file to replicate to all machines.
    job.setJarByClass(UserSessionsFilter.class);
    conf.set("mapreduce.user.classpath.first", "true");

    // Specify the Map
    job.setInputFormatClass(AvroKeyValueInputFormat.class);
    job.setMapperClass(UserSessionsFilterMapper.class);
    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

    // Specify input key schema for Avro input type.
    AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setInputValueSchema(job, Session.getClassSchema());

    // Specify output key schema for Avro input type
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Session.getClassSchema());

    // Specify output category
    AvroMultipleOutputs.addNamedOutput(job, SUBMITTER, AvroKeyValueOutputFormat.class,
        Schema.create(Schema.Type.STRING), Session.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, CLICKER, AvroKeyValueOutputFormat.class,
        Schema.create(Schema.Type.STRING), Session.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, SHOWER, AvroKeyValueOutputFormat.class,
        Schema.create(Schema.Type.STRING), Session.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, VISITOR, AvroKeyValueOutputFormat.class,
        Schema.create(Schema.Type.STRING), Session.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, OTHER, AvroKeyValueOutputFormat.class,
        Schema.create(Schema.Type.STRING), Session.getClassSchema());

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(appArgs[0]));
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

    int res = ToolRunner.run(new Configuration(), new UserSessionsFilter(), args);
    System.exit(res);
  }
}
