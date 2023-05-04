package pi.estimator;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.Parser.Token;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PiEstimator {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text circle = new Text("Circle");
        private Text square = new Text("Square");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ";");
			while (itr.hasMoreTokens()) {
                String tok = itr.nextToken().replace("(", "").replace(")", "");
                StringTokenizer newTok = new StringTokenizer(tok, ",");
                Double val1 = Double.parseDouble(newTok.nextToken());
                Double val2 = Double.parseDouble(newTok.nextToken());
                if(Math.pow(val1, 2) + Math.pow(val2, 2) <= 1) {
                    context.write(circle, one);
                }
				context.write(square, one);
			}
		}
	}


	public static class PiReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			System.out.println(result.toString());
			context.write(key, result);
		}

	}

	public static class ValueMapper
	extends Mapper<Object, Text, Text, Double>{

		private static ArrayList<Double> count = new ArrayList<Double>();		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
                StringTokenizer tok = new StringTokenizer(itr.nextToken().toString());
				String v1 = tok.nextToken();
                Double val1 = Double.parseDouble(tok.nextToken());
                count.add(val1);
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			Double piNum = 4*(count.get(0)/count.get(1));
			context.write(new Text("Pi is"), piNum);
		}
	}

    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "pointCounter");
		job.setJarByClass(PiEstimator.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(PiReducer.class);
		job.setReducerClass(PiReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "piCalculator");
		job2.setJarByClass(PiEstimator.class);
		job2.setMapperClass(ValueMapper.class);
		job2.setNumReduceTasks(0);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Double.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}