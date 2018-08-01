package km;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
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
public class Mark extends Configured implements Tool {
	
	public static class Map extends Mapper<Text, Text, Text, Text>{
		private HashMap<String, Vector<Double>> centers = new HashMap<>();
		private BufferedReader fis;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			Configuration conf = context.getConfiguration();
			String path = context.getLocalCacheFiles()[0].toString();//conf.get("answer2topic");
			readCenters(path);
	    }
		private void readCenters(String path) throws IOException {
			fis = new BufferedReader(new FileReader(path));
			String line = null;
			while ((line = fis.readLine()) != null) {
				Vector<Double> v = new Vector<>();
				String[] tmp = line.split("\t");
				String cid = tmp[0];
				for (String dim : tmp[1].split(" ")) {
					v.add(Double.parseDouble(dim));
				}
				centers.put(cid, v);
			}	
		}
		private double cos(Vector<Double> v1, Vector<Double> v2) {
			// The vector has been normalized
			double res = 0;
			for (int i = 0; i < v1.size(); i++) {
				res += v1.get(i) * v2.get(i);
			}
			return res;
		}
		private String closest(Vector<Double> v) {
			double max = -1;
			String result = "";
			Iterator<Entry<String, Vector<Double>>> iter = centers.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<String, Vector<Double>> entry = iter.next();
				double sim = cos(entry.getValue(), v);
				if (sim > max) {
					max = sim;
					result = entry.getKey();
				}
			}
			return result;
			
		}
		@Override
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			Vector<Double> v = new Vector<>();
			String[] dimensions = value.toString().split(" ");
			for (String dimension : dimensions) {
				v.add(Double.parseDouble(dimension));
			}
			String cen = closest(v);
			String str = value.toString() + "/" + cen;
			context.write(user, new Text(str));
			
		}
	}
	
	/**
	 * arg[0]: dataset
	 * arg[1]: last centers
	 * arg[2]: output
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "Mark");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		DistributedCache.addCacheFile(new Path(arg0[1]+"/part-r-00000").toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		
		job.setMapperClass(Map.class);
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
		int exitCode = ToolRunner.run(new Mark(), args);  
	  	  System.exit(exitCode);

	}

}
