import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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

public class ExtractUserTopicFeature extends Configured implements Tool {

	public ExtractUserTopicFeature() {
		// TODO Auto-generated constructor stub
	}

	public ExtractUserTopicFeature(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	
	public static class Map extends Mapper<Text, Text, Text, Text> {
		final java.util.Map<Integer, List> answer2topic = new HashMap<>();
		private BufferedReader fis;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			Configuration conf = context.getConfiguration();
			String path = context.getLocalCacheFiles()[0].toString();//conf.get("answer2topic");
			readAnswer2Topic(path);
	    }
		private void readAnswer2Topic(String path) throws IOException {
    			fis = new BufferedReader(new FileReader(path));
    			String line = null;
    			while ((line = fis.readLine()) != null) {
    				String[] tmp = line.split("\t");
    				String id = tmp[0];
    				if (tmp.length < 2) 
    					continue;
    				String[] topics = (tmp[1]).split(",");
    				List<Integer> lst = new ArrayList<>();
    				for (String topic : topics) {
    					lst.add(Integer.parseInt(topic));
    				}
    				answer2topic.put(Integer.parseInt(id), lst);
    			}
    		
		}
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			// <topicID, num of interactions>
			java.util.Map<Integer, Integer> userTopics = new HashMap<>();
			/*
			 * key: a user id. Unique
			 * value: ans1,ans2,ans3,...
			 */
			for (String answer : value.toString().split(",")) {
				int an_id = Integer.parseInt(answer);
				@SuppressWarnings("unchecked")
				List<Integer> topiclst = this.answer2topic.get(an_id);
				if (topiclst == null) continue;
				Iterator<Integer> iter = topiclst.iterator();
				while (iter.hasNext()) {
					int topicId = iter.next();
					if (userTopics.containsKey(topicId)) {
						int old_num = userTopics.get(topicId);
						userTopics.put(topicId, old_num+1);
					} else {
						userTopics.put(topicId, 1);
					}
				}
			}
			
			StringBuilder buffer = new StringBuilder();
			List<Entry<Integer, Integer>> entryList = new ArrayList<>(userTopics.entrySet());
			Collections.sort(entryList, new Comparator<Entry<Integer, Integer>>() {
			    public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
			        return ((Entry<Integer, Integer>) o2).getValue()
			                   - (((Entry<Integer, Integer>) o1).getValue());
			    }
			});
			Iterator<Entry<Integer, Integer>> iter = entryList.iterator();
			while (iter.hasNext()) {
				Entry<Integer, Integer> entry = iter.next();
				buffer.append(entry.getKey());
				buffer.append(' ');
				buffer.append(entry.getValue());
				buffer.append(',');
			}
			context.write(user, new Text(buffer.toString()));
		}
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 * arg[0]: user2answers/
	 * arg[1]: answer2topics/  33M  cache
	 * arg[2]: output dir
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Extract user-topic features");
		job.setJarByClass(this.getClass());
		
		DistributedCache.addCacheFile(new Path(arg0[1]).toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		job.getConfiguration().set("answer2topic", arg0[1]);
		
		FileSystem fs=FileSystem.get(getConf());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		
		
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		int val= job.waitForCompletion(true) ? 0 : 1;
		
		return val;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ExtractUserTopicFeature(), args);  
	  	  System.exit(exitCode);

	}

}
