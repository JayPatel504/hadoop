import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class taxi {

	public static class Dict implements Comparable<Dict> {
		double val;
		String key;

		public Dict(double val, String key) {
			this.val = val;
			this.key = key;
		}

		public int compareTo(Dict d) {
			if (this.val <= d.val) {
				return 1;
			} 
			else {
				return -1;
			}
		}
	}	
	public static TreeSet<Dict> high = new TreeSet<Dict>();
	public static TreeSet<Dict> low = new TreeSet<Dict>();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		private LongWritable temp = new LongWritable(1);
		
		public static boolean isInt(String s) {
			boolean vInt = false;
			try {
				Integer.parseInt(s);
				vInt = true;
			} 
			catch (NumberFormatException ex) {
			}
			return vInt;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String col[] = value.toString().split(",");

			if (isInt(col[19]) && isInt(col[20])) {
				Text ori = new Text("* " + col[16]);
				context.write(ori, new LongWritable(Integer.parseInt(col[19])));
				Text dep = new Text("* " + col[17]);
				context.write(dep, new LongWritable(Integer.parseInt(col[20])));
			}
			Text ori = new Text(col[16]);
			context.write(ori, temp);
			Text dep = new Text(col[17]);
			context.write(dep, temp);
		}
	}

	public static class Com extends Reducer<Text, LongWritable, Text, LongWritable> {
	
		public void combine(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable v : values) {
				sum += v.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static class Red extends Reducer<Text, LongWritable, Text, Text> {
		private HashMap<String, Double> hashMap = new HashMap<String, Double>();
		private double getCount(Iterable<LongWritable> values) {
			double c = 0;
			for (LongWritable v : values) {
				c += v.get();
			}
			return c;
		}

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			
			if (key.toString().split(" ")[0].equals("*")) {
				String w = key.toString().split(" ")[1];
				double wf = getCount(values);
				hashMap.put(w, wf);
			} 
			else {
				String w = key.toString().split(" ")[0];
				double pf = getCount(values);
				if((hashMap.get(w))!=null){
					double wf = hashMap.get(w);
					DoubleWritable freq = new DoubleWritable();
					freq.set((wf/pf));
					Double Finalfreq = freq.get();
					high.add(new Dict(Finalfreq, key.toString()));
					low.add(new Dict(Finalfreq, key.toString()));
					if (high.size() > 3) {
						high.pollLast();
					}
					if (low.size() > 3) {
						low.pollFirst();
					}
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("High Taxi Avg:  "), null);
			while (!high.isEmpty()) {
				Dict pair = high.pollFirst();
				context.write(new Text(pair.key), new Text(Double.toString(pair.val)));
			}
			context.write(new Text("Low Taxi Avg:  "), null);
			while (!low.isEmpty()) {
				Dict pair = low.pollLast();
				context.write(new Text(pair.key), new Text(Double.toString(pair.val)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Taxi Stats");
		job.setJarByClass(taxi.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Com.class);
		job.setReducerClass(Red.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}

}
