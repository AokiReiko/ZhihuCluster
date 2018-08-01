package cluster;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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


public class KNN extends Configured implements Tool {

	public static class Map extends Mapper<Text, Text, Text, Text>{
		private ArrayList<String> tag = new ArrayList<>();
		private ArrayList<Vector<Double>> vecs = new ArrayList<>();
		private BufferedReader fis;
		private final int K = 30;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			Configuration conf = context.getConfiguration();
			String path = context.getLocalCacheFiles()[0].toString();//conf.get("answer2topic");
			readMarkedSet(path);
	    }
		private void readMarkedSet(String path) throws IOException {
			fis = new BufferedReader(new FileReader(path));
			String line = null;
			while ((line = fis.readLine()) != null) {
				Vector<Double> v = new Vector<>();
				String[] tmp = line.split("\t");
				tmp = tmp[1].split(" ");
				for (int i = 0; i < tmp.length - 1; i++) {
					v.add(Double.parseDouble(tmp[i]));
				}
				vecs.add(v);
				tag.add(tmp[tmp.length-1].replaceAll("/", ""));
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
		@Override
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			Vector<Double> v = new Vector<>();
			String[] dimensions = value.toString().split(" ");
			for (String dimension : dimensions) {
				v.add(Double.parseDouble(dimension));
			}
			ArrayList<Pair<Integer, Double>> lst = new ArrayList<>();
			for (int i = 0; i < tag.size(); i++) {
				lst.add(new Pair<Integer, Double>(i, cos(v, vecs.get(i))));
			}
			Collections.sort(lst,new Comparator<Pair<Integer,Double>>() {
				@Override
				public int compare(Pair<Integer,Double> o1, Pair<Integer,Double> o2) {
					return o2.getValue().compareTo(o2.getValue());
				}    
	        });
			HashMap<String, Integer> k_res = new HashMap<>();
			for (int i = 0; i < K; i++) {
				String cid = tag.get(lst.get(i).getKey());
				if (k_res.containsKey(cid)) {
					k_res.put(cid, k_res.get(cid)+1);
				} else{ 
					k_res.put(cid, 1);
				}
			}
			Iterator<Entry<String, Integer>> iter = k_res.entrySet().iterator();
			int max = 0;
			String hot = null;
			while(iter.hasNext()) {
				Entry<String, Integer> entry = iter.next();
				if (max < entry.getValue()) {
					max = entry.getValue();
					hot = entry.getKey();
				}
			}
			context.write(new Text(hot), user);	
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text cid, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder buffer = new StringBuilder();
			for (Text value: values) {
				context.write(cid, value);
			}
			
			
		}
	}
	/**
	 * arg[0]: dataset, normalizedUserVec
	 * arg[1]: trainset marked
	 * arg[2]: output
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "KNN");
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		DistributedCache.addCacheFile(new Path(arg0[1]+"/part-r-00000").toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
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
		int exitCode = ToolRunner.run(new KNN(), args);  
	  	  System.exit(exitCode);

	}

}
