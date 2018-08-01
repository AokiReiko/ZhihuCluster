package km;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * Pick k init centers
 * @author aokireiko
 *
 */
public class Init extends Configured implements Tool {

	public Init() {
		// TODO Auto-generated constructor stub
	}

	public Init(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	
	public static class Map extends Mapper<Text, Text, Text, Text>{
		private final int K = 20;
		private final int N = 1000;
		private int count = 0;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			
	    }
		private double cos(Vector<Double> v1, Vector<Double> v2) {
			// The vector has been normalized
			double res = 0;
			for (int i = 0; i < v1.size(); i++) {
				res += v1.get(i) * v2.get(i);
			}
			return res;
		}
		
		@Override
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			Vector<Double> v = new Vector<>();
			String[] dimensions = value.toString().split(" ");
			for (String dimension : dimensions) {
				v.add(Double.parseDouble(dimension));
			}
			double ran = Math.random();
			if (ran <= (K+0.0) / N) {
				context.write(user, value);
			}
		}
	}
	/**
	 * 
	 * @param arg0[0]: samples
	 * arg0[1]: initKcenters->iter0 (for example)
	 * @return
	 * @throws Exception
	 */
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "Init k centers");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		int val = job.waitForCompletion(true) ? 0 : 1;
		return val;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Init(), args);  
	  	  System.exit(exitCode);

	}

}
