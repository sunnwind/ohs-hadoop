package ohs.task.news;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class HadoopDataHandler extends Configured implements Tool {

	public static class MMaper extends Mapper<Object, Text, Text, Text> {

		private Text input = new Text();

		private Text output = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = null;
			try {
				jsonObject = (JSONObject) jsonParser.parse(line);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// labels: id, source, published, title, media-type, content
			List<String> labels = new ArrayList<String>();
			List<String> contents = new ArrayList<String>();

			for (Object k : jsonObject.keySet()) {
				labels.add(k.toString());
				String v = jsonObject.get(k).toString().replace("\r", "").replace("\n", "\\n");
				v = v.replaceAll("[\\s]+", " ").trim();
				contents.add(v);
			}

			// String content = contents.get(5).replace("\\n", "\n");
			System.out.println(contents.get(5));

			Annotation anno = nlp.process(contents.get(5).replace("\\n", "\n"));

			ByteArrayOutputStream os = new ByteArrayOutputStream();
			nlp.xmlPrint(anno, os);
			contents.add(os.toString().replace("\r\n", "\\n"));
			String id = contents.get(0);
			input.set(id);
			output.set(String.join("\t", contents));
			context.write(input, output);

		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class RReducer extends Reducer<Text, Text, Text, Text> {
		private Text input = new Text();
		private Text output = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, ArrayIndexOutOfBoundsException {
			System.out.println("## Reducer ##");
			for (Text value : values) {
				input.set(key.toString());
				output.set(value.toString());
				context.write(key, output);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private static StanfordCoreNLP nlp = new StanfordCoreNLP(getProps());

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

	public static void deleteFilesUnder(File dir) {
		int numFiles = deleteFiles(dir);
		System.out.println(String.format("delete [%d] files under [%s]", numFiles, dir.getPath()));
	}

	public static void deleteFilesUnder(String dirName) {
		deleteFiles(new File(dirName));
	}

	private static Properties getProps() {
		Properties ret = new Properties();
		// ret.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner,
		// parse, sentiment");
		ret.setProperty("parse.maxlen", "100");
		ret.setProperty("annotators", "tokenize, ssplit, pos");
		return ret;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("process begins.");
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

		String[] otherArgs = new String[0];

		int res = ToolRunner.run(conf, new HadoopDataHandler(), args);

		// if (otherArgs.length == 0) {
		otherArgs = new String[2];
		// otherArgs[0] = "/data1/ohs/data/news_ir/sample-1M_subset.jsonl";
		// otherArgs[1] = "/data1/ohs/data/news_ir/sample-1M/";
		// } else {
		// otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// if (otherArgs.length != 2) {
		// System.err.println("Usage: nlp <in> <out>");
		// System.exit(2);
		// }
		// }

		// conf.set("fs.defaultFS", "file:///");
		// conf.set("mapred.job.tracker", "local");
		// conf.set("fs.file.impl", "WindowsLocalFileSystem");
		// conf.set("io.serializations",
		// "org.apache.hadoop.io.serializer.JavaSerialization,"
		// + "org.apache.hadoop.io.serializer.WritableSerialization");

		System.out.println("process ends.");
	}

	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path(args[1]), true);

		JobConf jb = new JobConf(getConf());
		Job job = Job.getInstance(jb);
		job.setJobName("nlp");
		job.setJarByClass(HadoopDataHandler.class);
		job.setMapperClass(MMaper.class);
		job.setReducerClass(RReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.out.printf("input:\t%s\n", args[0]);
		System.out.printf("output:\t%s\n", args[0]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// deleteFilesUnder(otherArgs[1]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}
