package ohs.task.news;

import java.io.File;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, value);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				context.write(key, val);
			}

		}
	}

	private static int deleteFiles(File root) {
		int numFiles = 0;
		if (root.exists()) {
			if (root.isDirectory()) {
				for (File child : root.listFiles()) {
					numFiles += deleteFiles(child);
				}
				root.delete();
			} else if (root.isFile()) {
				root.delete();
				numFiles++;
			}
		}
		return numFiles;
	}

	private static Properties getProps() {
		Properties ret = new Properties();
		// ret.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner,
		// parse, sentiment");
		ret.setProperty("parse.maxlen", "100");
		ret.setProperty("annotators", "tokenize, ssplit, pos");
		return ret;
	}

	public static void deleteFilesUnder(File dir) {
		int numFiles = deleteFiles(dir);
		System.out.println(String.format("delete [%d] files under [%s]", numFiles, dir.getPath()));
	}

	public static void deleteFilesUnder(String dirName) {
		deleteFiles(new File(dirName));
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// if (otherArgs.length < 2) {
		// System.err.println("Usage: wordcount <in> [<in>...] <out>");
		// System.exit(2);
		// }

		String[] otherArgs = new String[2];
		otherArgs[0] = "/data1/ohs/data/news_ir/sample-1M_subset.jsonl";
		otherArgs[1] = "/data1/ohs/data/news_ir/sample-1M/";

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		deleteFilesUnder(otherArgs[1]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
