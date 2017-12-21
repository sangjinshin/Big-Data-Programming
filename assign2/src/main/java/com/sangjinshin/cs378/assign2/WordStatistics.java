package com.sangjinshin.cs378.assign2;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
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

/**
 * 
 * CS 378 Big Data Programming Assignment 2 -<br>
 * MapReduce program that computes mean and variance
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 * 
 */

public class WordStatistics {

  /**
   * 
   * The Mapper class for WordStatistics
   * 
   * The mapper will process each line of text from input data to calculate the frequency (number of
   * times in a paragraph) of each word. The output key is a word. The output value is
   * WordStatisticsWritable containing two Long values document count and frequency of that word.
   *
   */
  public static class WordStatisticsMapper
      extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

    private static final long ONE = 1L;
    private Text word = new Text();
    private WordStatisticsWritable outWordStatistics;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      // Sorted map of <word, WordStatisticsWritable setting doc_count and word_freq>
      TreeMap<String, Long> wordMap = new TreeMap<String, Long>();

      // Grab a line of text
      String line = value.toString();

      // Convert text to lowercase
      line = line.toLowerCase();
      // Remove everything except a-z, Latin letters, and footnote citations
      line = line.replaceAll("[^a-z\\p{L}\\d\\[\\]'*\\\\1* ]", " ");
      // Adds a space before "[" for footnote citations that gets
      // tokenized as part of a word e.g. "Columbus[273]"
      line = line.replaceAll("[\\[]", " [");

      // Tokenize the input line to iterate over each word
      StringTokenizer tokenizer = new StringTokenizer(line);

      // For each word in the input line, create WordStatisticsWritable
      // object and set doc_count and word_freq
      while (tokenizer.hasMoreTokens()) {
        // Get the next token in the input line
        String token = tokenizer.nextToken();

        // Create new k/v pair if none exists
        if (!wordMap.containsKey(token)) {
          wordMap.put(token, ONE);
        }

        // Update the value (WordStatisticsWritable->word_freq) if the key already
        // exists
        else {
          wordMap.put(token, wordMap.get(token) + 1);
        }
      }

      // Iterate the key (token/word) in the wordMap
      for (String token : wordMap.keySet()) {
        // Set the Text object "word" to the key "token"
        word.set(token);

        // Set values from wordMap
        outWordStatistics = new WordStatisticsWritable();
        outWordStatistics.setDocCount(ONE);
        long word_freq = wordMap.get(token);
        outWordStatistics.setWordFreq(word_freq);
        outWordStatistics.setWordFreqSquared(); // necessary to calculate variance later

        // Write out the resulting <Text, WordStatisticsWritable> pair
        context.write(word, outWordStatistics);
      }
    }
  }

  /**
   * 
   * The Combiner class for WordStatistics
   * 
   * The combiner code will aggregate WordStatistics entries for each local map's intermediate
   * key/value pairs.
   *
   */
  public static class WordStatisticsCombiner
      extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

    private WordStatisticsWritable outWordStatistics = new WordStatisticsWritable();

    @Override
    protected void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
        throws IOException, InterruptedException {

      long doc_count = 0;
      long word_freq = 0;
      long word_freq_squared = 0;

      for (WordStatisticsWritable v : values) {
        doc_count += v.getDocCount();
        word_freq += v.getWordFreq();
        word_freq_squared += v.getWordFreqSquared();

      }
      outWordStatistics.setDocCount(doc_count);
      outWordStatistics.setWordFreq(word_freq);
      outWordStatistics.setWordFreqSquared(word_freq_squared);

      // Write out the resulting k/v pair
      context.write(key, outWordStatistics);
    }
  }

  /**
   * 
   * The Reducer class for WordStatistics
   * 
   * The reducer class iterates through given WordStatisticsWritable to aggregate all the maps
   * together into a single WordStatisticsWritable. The key is each word and the value is
   * WordStatisticsWritable containing values document count, word frequency, word frequency squared
   * (needed to be stored to calculate variance).
   * 
   * After iteration, the mean and the variance are calculated as follows:
   * 
   * mean = word_freq / doc_count
   * 
   * variance = mean of squares - square of means = (word_freq_squared / doc_count) - (mean * mean)
   *
   */
  public static class WordStatisticsReducer
      extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

    private WordStatisticsWritable outWordStatistics = new WordStatisticsWritable();

    @Override
    public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
        throws IOException, InterruptedException {

      long doc_count = 0;
      long word_freq = 0;
      long word_freq_squared = 0;
      double mean = 0.0;
      double variance = 0.0;

      for (WordStatisticsWritable v : values) {
        doc_count += v.getDocCount();
        word_freq += v.getWordFreq();
        word_freq_squared += v.getWordFreqSquared();
      }
      outWordStatistics.setDocCount(doc_count);
      outWordStatistics.setWordFreq(word_freq);
      outWordStatistics.setWordFreqSquared(word_freq_squared);

      // Calculate mean
      mean = (double) word_freq / doc_count;
      outWordStatistics.setMean(mean);

      // Calculate variance
      variance = ((double) word_freq_squared / doc_count) - (mean * mean);
      outWordStatistics.setVariance(variance);

      // Write out the resulting k/v pair
      context.write(key, outWordStatistics);
    }
  }

  /**
   * 
   * Specify characteristics of MapReduce job and initiate
   * 
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "wordstatistics");
    // Identify the JAR file to replicate to all machines.
    job.setJarByClass(WordStatistics.class);

    // Set the output key and value types (for map and reduce).
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(WordStatisticsWritable.class);

    // Set the map and reduce classes.
    job.setMapperClass(WordStatisticsMapper.class);
    job.setCombinerClass(WordStatisticsCombiner.class);
    job.setReducerClass(WordStatisticsReducer.class);

    // Set the input and output file formats.
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Grab the input file and output directory from the command line.
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Initiate the map-reduce job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
