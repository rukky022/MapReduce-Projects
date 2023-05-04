package pagerank.mc;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMC {
	//Change this value depending on the number of simulations
	private static Double simulations = 10000000.0;

	public static class PathMapper
			extends Mapper<Object, Text, IntWritable, DoubleWritable> {

		private Map<Integer, ArrayList<Integer>> adjacencyVals = new HashMap<Integer, ArrayList<Integer>>();

		@Override
		protected void setup(
				Mapper<Object, Text, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			if (context.getCacheFiles() != null
					&& context.getCacheFiles().length > 0) {
				URI[] cacheFiles = context.getCacheFiles();
				String sCacheFileName = cacheFiles[0].toString();
				FileSystem aFileSystem = FileSystem.get(context.getConfiguration());
				Path aPath = new Path(sCacheFileName);
				BufferedReader br = new BufferedReader(new InputStreamReader(aFileSystem.open(aPath)));
				String line;
				System.out.println("Adjacency Graph");
				// Read the Adjacency values of all nodes in this iteration.
				while ((line = br.readLine()) != null) {
					// process the line.
					Integer nOneNodeIndex = 0;
					Integer neighborNode = 0;
					StringTokenizer itr = new StringTokenizer(line);
					nOneNodeIndex = Integer.parseInt(itr.nextToken());
					adjacencyVals.put(nOneNodeIndex, new ArrayList<Integer>());
					while (itr.hasMoreTokens()) {
						neighborNode = Integer.parseInt(itr.nextToken());
						adjacencyVals.get(nOneNodeIndex).add(neighborNode);
						System.out.println(nOneNodeIndex + " " + neighborNode);
					}
				}
			}
			super.setup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			ArrayList<Integer> z_arr = new ArrayList<Integer>();
			Integer a = Integer.parseInt(value.toString());
			boolean pathSuccess = true;
			z_arr.add(1);

			for (int t = 1; t < a; t++) {
				//Get the neighbording nodes of the node preceding the current node
				ArrayList<Integer> adjArr = adjacencyVals.get(z_arr.get(t - 1));

				if (adjArr.size() == 0) {
					pathSuccess = false;
					break;
				}
				
				//Choose a random node from the neighbording nodes
				Integer randomNum = 0;
				if (adjArr.size() > 1) {
					randomNum = ThreadLocalRandom.current().nextInt(0, adjArr.size());
				}
				Integer node = adjArr.get(randomNum);
				System.out.println("Zt, Random Node " + t + ", " + node);
				z_arr.add(node);   //Add it to the array at position t (basically end of the array)
			}

			if (pathSuccess) {
				//If the path length was 0, then just get Node 1
				if (a > 0) {
					System.out.println("End Node " + z_arr.get(a-1));
					context.write(new IntWritable(z_arr.get(a-1)), new DoubleWritable(1.0));
				} else {
					System.out.println("End Node " + z_arr.get(a));
					context.write(new IntWritable(z_arr.get(a)), new DoubleWritable(1.0));
				}

			}

		}
	}

	public static class RankReducer
			extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		private DoubleWritable prValue = new DoubleWritable();

		public void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			//Calculate the PR values for each node
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			prValue.set(sum / simulations);

			System.out.println("Node, PR Value " + key + " " + prValue);
			context.write(key, prValue);
		}

	}

	public static void main(String[] args) throws Exception {
		String aValues = args[0];   //file containing the path lengths
		String adjacencyList = args[1];  //file containing the adjacency list
		String outputPath = args[2];  //path for output

		//Was unable to succesfully send the number of simulations to run to the Reducer function
		System.out.println("Simulation Count: " + args[3]);
		simulations = Double.parseDouble(args[3]);
		System.out.println(simulations);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PR Monte Carlo Method");

		job.setJarByClass(PageRankMC.class);
		job.setMapperClass(PathMapper.class);
		job.setReducerClass(RankReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(aValues));
		job.addCacheFile(new Path(adjacencyList).toUri());
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}