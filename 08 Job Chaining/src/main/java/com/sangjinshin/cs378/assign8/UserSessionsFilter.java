package com.sangjinshin.cs378.assign8;

import java.io.IOException;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.sessions.SessionType;

/**
 * CS 378 Big Data Programming Assignment 8 - Job Chaining
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 */

/**
 * Binning Class that filter to different bins based on Session EventType
 */
public final class UserSessionsFilter {

  private static final long ONE = 1L;

  // Counters
  public static final String WRITE_COUNTER = "write counter";
  public static final String DISCARD_COUNTER = "discard counter";

  // SessionType enums
  public static final String SUBMITTER = SessionType.SUBMITTER.getText();
  public static final String CLICKER = SessionType.CLICKER.getText();
  public static final String SHOWER = SessionType.SHOWER.getText();
  public static final String VISITOR = SessionType.VISITOR.getText();
  public static final String OTHER = SessionType.OTHER.getText();

  /**
   * Binning Mapper
   */
  public static class UserSessionsFilterMapper extends
      Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

    private AvroMultipleOutputs multipleOutputs = null;

    @Override
    protected void setup(Context context) {
      multipleOutputs = new AvroMultipleOutputs(context);
    }

    @Override
    protected void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
        throws IOException, InterruptedException {
      Session session = value.datum();

      boolean isSubmitter = false;
      boolean isClicker = false;
      boolean isShower = false;
      boolean isVisitor = false;
      boolean isOther = false;

      if (session.getEvents().size() > 100) { // filter out session with more than 100 events
        context.getCounter(DISCARD_COUNTER, "Large Session Discarded").increment(ONE);
      } else {
        for (Event e : session.getEvents()) {
          EventType eventType = e.getEventType();
          if ((eventType == EventType.CHANGE || eventType == EventType.EDIT
              || eventType == EventType.SUBMIT)) {
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
          context.getCounter(WRITE_COUNTER, SUBMITTER).increment(ONE);
          multipleOutputs.write(SUBMITTER, key, value);
        }
        if (isClicker) {
          context.getCounter(WRITE_COUNTER, CLICKER).increment(ONE);
          multipleOutputs.write(CLICKER, key, value);
        }
        if (isShower) {
          context.getCounter(WRITE_COUNTER, SHOWER).increment(ONE);
          multipleOutputs.write(SHOWER, key, value);
        }
        if (isVisitor) {
          context.getCounter(WRITE_COUNTER, VISITOR).increment(ONE);
          multipleOutputs.write(VISITOR, key, value);
        }
        if (isOther) {
          context.getCounter(WRITE_COUNTER, OTHER).increment(ONE);
          multipleOutputs.write(OTHER, key, value);
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws InterruptedException, IOException {
      multipleOutputs.close();
    }
  }
}
