package com.sangjinshin.cs378.assign2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * 
 * CS 378 Big Data Programming Assignment 2 -<br>
 * Custom writable interface for WordStatistics
 *
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96<br>
 * 
 */

public class WordStatisticsWritable implements Writable {
  private long doc_count;
  private long word_freq;
  private long word_freq_squared;

  private double mean;
  private double variance;

  public WordStatisticsWritable() {
    this.doc_count = 0;
    this.word_freq = 0;
    this.word_freq_squared = 0;
    this.mean = 0.0;
    this.variance = 0.0;
  }

  public long getDocCount() {
    return this.doc_count;
  }

  public void setDocCount(long doc_count) {
    this.doc_count = doc_count;
  }

  public long getWordFreq() {
    return this.word_freq;
  }

  public void setWordFreq(long word_freq) {
    this.word_freq = word_freq;
  }

  public long getWordFreqSquared() {
    return this.word_freq_squared;
  }

  public void setWordFreqSquared() {
    this.word_freq_squared = this.word_freq * this.word_freq;
  }

  public void setWordFreqSquared(long word_freq_squared) {
    this.word_freq_squared = word_freq_squared;
  }

  public double getMean() {
    return this.mean;
  }

  public void setMean(double mean) {
    this.mean = mean;
  }

  public double getVariance() {
    return this.variance;
  }

  public void setVariance(double variance) {
    this.variance = variance;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.doc_count = in.readLong();
    this.word_freq = in.readLong();
    this.word_freq_squared = in.readLong();
    this.mean = in.readDouble();
    this.variance = in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.doc_count);
    out.writeLong(this.word_freq);
    out.writeLong(this.word_freq_squared);
    out.writeDouble(this.mean);
    out.writeDouble(this.variance);
  }

  @Override
  public String toString() {
    return "\t" + doc_count + "," + mean + "," + variance;
  }
}
