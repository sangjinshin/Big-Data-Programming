package com.sangjinshin.cs378.assign4;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import com.refactorlabs.cs378.utils.Utils;

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

/**
 * 
 * CS 378 Big Data Programming Assignment 4 -<br>
 * MapReduce Program that computes statistics for words using AVRO
 * 
 * @author Sangjin Shin (sangjinshin@outlook.com)<br>
 *         UTEID: ss62273<br>
 *         CSID: sshin96
 * 
 */

public class WordStatisticsAvro extends Configured implements Tool {

	/**
	 * 
	 * The Mapper class for WordStatisticsAvro
	 * 
	 * The mapper will process each line of text from input data to store the number
	 * of lines each word appears in (document_count) as well as the number of times
	 * the word appears in each line (total_count).
	 * 
	 * The output key is Text word. The output value is AVRO object containing
	 * document_count and total_count.
	 *
	 */
	public static class WordStatisticsMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

		private static final long ONE = 1L;

		/**
		 * Local variable "word" will contain the word identified in the input.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Sorted map of <word, total_count>
			TreeMap<String, Long> wordMap = new TreeMap<String, Long>();

			// Grab a line of text
			String line = value.toString();

			// Convert text to lowercase
			line = line.toLowerCase();

			// Tokenize the input line to iterate over each word
			StringTokenizer tokenizer = new StringTokenizer(line);

			// For each word in the input line, pair with total_count
			// Prevent duplicates
			while (tokenizer.hasMoreTokens()) {
				// Get the next token in the input line
				String token = tokenizer.nextToken();

				// Create new k/v pair if none exists
				if (!wordMap.containsKey(token)) {
					wordMap.put(token, ONE);
				}

				// Update existing k/v pair
				else {
					wordMap.put(token, wordMap.get(token) + 1);
				}
			}

			// Iterate through key (word) in the map and build AVRO object
			for (String token : wordMap.keySet()) {
				// Set token to Text object
				word.set(token);

				WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();

				// Store values from wordMap into builder
				builder.setDocumentCount(ONE);
				long total_count = wordMap.get(token);
				builder.setTotalCount(total_count);
				builder.setSumOfSquares(total_count * total_count);
				builder.setMin(total_count);
				builder.setMax(total_count);

				// Correct values will be set in reduce class, set to 0 for now
				builder.setMean(0.0);
				builder.setVariance(0.0);

				// Write output to context
				context.write(word, new AvroValue<WordStatisticsData>(builder.build()));
			}
		}
	}

	/**
	 * 
	 * The Reducer class for WordStatisticsAvro
	 * 
	 * The reducer class iterates through AVRO objects to aggregate all the values
	 * together into a single AVRO object.
	 * 
	 * The output key is the word. The output value is AVRO object containing all
	 * the statistics of each word.
	 * 
	 * Mean and variance are calculated using respective formula as follows:
	 * 
	 * mean = total_count / document_count
	 * 
	 * variance = mean of squares - square of means<br>
	 * = (sum_of_squares / document_count) - (mean * mean)
	 *
	 */
	public static class WordStatisticsReducer
			extends Reducer<Text, AvroValue<WordStatisticsData>, Text, AvroValue<WordStatisticsData>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
				throws IOException, InterruptedException {

			/**
			 * Local variables used to aggregate AvroValues
			 */
			long document_count = 0;
			long total_count = 0;
			long sum_of_squares = 0;
			long min = 0;
			long max = 0;
			double mean = 0.0;
			double variance = 0.0;

			// Sum up the statistics for the current word, specified in object "key"
			for (AvroValue<WordStatisticsData> v : values) {
				document_count += v.datum().getDocumentCount();
				total_count += v.datum().getTotalCount();
				sum_of_squares += v.datum().getSumOfSquares();
				if (min == 0 || v.datum().getMin().compareTo(min) < 0) {
					min = v.datum().getMin();
				}
				if (max == 0 || v.datum().getMax().compareTo(max) > 0) {
					max = v.datum().getMax();
				}
			}

			// Calculate mean and variance
			mean = (double) total_count / document_count;
			variance = ((double) sum_of_squares / document_count) - (mean * mean);

			WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();

			// Set statistics of each word in builder
			builder.setDocumentCount(document_count);
			builder.setTotalCount(total_count);
			builder.setSumOfSquares(sum_of_squares);
			builder.setMin(min);
			builder.setMax(max);
			builder.setMean(mean);
			builder.setVariance(variance);

			// Write out results to context
			context.write(key, new AvroValue<WordStatisticsData>(builder.build()));
		}
	}

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordStatisticsAvro <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "WordStatisticsAvro");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatisticsData.class);

		// Specify the Map
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(WordStatisticsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(WordStatisticsReducer.class);
		job.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job by
	 * setting values on the Job object, and then initiates the map-reduce job and
	 * waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new WordStatisticsAvro(), args);
		System.exit(res);
	}
}
