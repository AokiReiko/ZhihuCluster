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

public class VectorizeUsers extends Configured implements Tool {
	
	public static class Map extends Mapper<Text, Text, Text, Text> {
		final java.util.Map<Integer, Vector<Double>> topicVec = new HashMap<>();
		private BufferedReader fis;
		private int di_num = -1;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			Configuration conf = context.getConfiguration();
			String path = context.getLocalCacheFiles()[0].toString();//conf.get("answer2topic");
			readTopicVec(path);
	    }
		private void readTopicVec(String path) throws IOException {
    			fis = new BufferedReader(new FileReader(path));
    			String line = null;
    			while ((line = fis.readLine()) != null) {
    				String[] tmp = line.split("\t");
    				int id = Integer.parseInt(tmp[0]);
    				if (tmp.length < 2) 
    					continue;
    				String[] dimensions = (tmp[1]).split(" ");
    				if (di_num == -1) {
    					di_num = dimensions.length;
    				}
    				Vector<Double> v = new Vector<>(dimensions.length);
    				for (String dimension : dimensions) {
    					v.add(Double.parseDouble(dimension));
    				}
    				topicVec.put(id, v);
    			}	
		}
		private void addVec(Vector<Double> v1, Vector<Double> v2, Double w) {
			if (v1 == null || v2 == null || v1.size() != v2.size()) {
				return;
			}
			for (int i = 0; i < v1.size(); i++) {
				double d = v1.get(i) + v2.get(i) * w;
				v1.set(i, d);
			}
		}
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			Vector<Double> v = new Vector<>();
			v.setSize(di_num);
			for (int i = 0; i < di_num; i++) {
				v.set(i, (double) 0);
			}
			String[] topics = value.toString().split(",");
			for (String topic : topics) {
				String [] tmp = topic.split(" ");
				if (tmp[0].isEmpty()) {
					continue;
				}
				int tid = Integer.parseInt(tmp[0]);
				int num = Integer.parseInt(tmp[1]);
				double w = Math.log(num);
				addVec(v, topicVec.get(tid), w);
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
		Job job = Job.getInstance(getConf(), "Convert user-topic features to user-Vec");
		job.setJarByClass(this.getClass());
		
		DistributedCache.addCacheFile(new Path(arg0[1]).toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		
		
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
		int exitCode = ToolRunner.run(new VectorizeUsers(), args);  
	  	  System.exit(exitCode);

	}

}
