import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class cancel {

	public static class Dict implements Comparable<Dict> {
		int val;
		String key;

		public Dict(String key, long val) {
			this.val = (int) val;
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
	public static TreeSet<Dict> reas = new TreeSet<Dict>();

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
			String c = col[22];
			String re = "N/A";
			String can = col[21];
			if (can.equals("1") && !c.equals("NA")) {
				switch(c) {
					case "A":
						re = "carrier";
						break;
					case "B":
						re = "weather";
						break;
					case "C":
						re = "NAS";
						break;
					case "D":
						re = "security";
						break;
				}
				context.write(new Text(re), temp);
			}
		}
	}

	public static class Red extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable v : values) {
				sum += v.get();
			}
			reas.add( new Dict(key.toString(), sum) );
			if(reas.size() > 1){
				reas.pollLast();
			}			    	
		}

	    protected void cleanup(Context context) throws IOException, InterruptedException {
	    	context.write(new Text("Top Cancellation Reason:  "), null);
	    	while(!reas.isEmpty()){
				Dict pair = reas.pollFirst();
	    		context.write(new Text(pair.key), new LongWritable(pair.val));
	    	}
	    }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Flight Canccellations");
		job.setJarByClass(cancel.class);
		job.setMapperClass(Map.class);
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