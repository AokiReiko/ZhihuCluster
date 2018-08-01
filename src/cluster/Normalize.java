package cluster;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Normalize extends Configured implements Tool {
	
	public static class Map extends Mapper<Text, Text, Text, Text> {
		private int di_num = 50;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			Configuration conf = context.getConfiguration();
			
	    }
	
		private boolean normalize(Vector<Double> v) {
			double length = 0;
			for (Double d : v) {
				length += d * d;
			}
			length = Math.sqrt(length);
			for (int i = 0; i < v.size(); i++) {
				v.set(i, v.get(i) / length);
			}
			return (length != 0);
		}
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			Vector<Double> v = new Vector<>();
			String[] dimensions = value.toString().split(" ");
			for (String dimension : dimensions) {
				double w = Double.parseDouble(dimension);
				v.add(w);
			}
			if (!normalize(v)) {
				return;
			}
			StringBuilder buffer = new StringBuilder();
			for (Double d : v) {
				buffer.append(d);
				buffer.append(" ");
			}
			context.write(user, new Text(buffer.toString()));
		}
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 * arg[0]: user2Topics/
	 * arg[1]: index_vectors.txt/  43M  cache
	 * arg[2]: output dir
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Normalize user-Vec");
		job.setJarByClass(this.getClass());
				
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
		int val= job.waitForCompletion(true) ? 0 : 1;
		
		return val;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Normalize(), args);  
	  	  System.exit(exitCode);

	}

}
