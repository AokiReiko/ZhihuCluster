import java.io.IOException;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class Answer2Topic extends Configured implements Tool {

	public Answer2Topic() {
		// TODO Auto-generated constructor stub
	}

	public Answer2Topic(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		final java.util.Map<String, Integer> answer_dict = new HashMap<>();
		final java.util.Map<String, Integer> topic_dict = new HashMap<>();
		private BufferedReader fis;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String an_path = conf.get("answer_dict");
			String to_path = conf.get("topic_dict");
			readAnswerDict(an_path);
			readTopicDict(to_path);
			
		}
		private void readAnswerDict(String path) {
			try {
    			fis = new BufferedReader(new FileReader(path));
    			String line = null;
    			while ((line = fis.readLine()) != null) {
    				String[] tmp = line.split("\t");
    				answer_dict.put(tmp[1], Integer.parseInt(tmp[0]));
    			}
    		} catch (IOException ioe) {
    			System.err.println("Exception while reading answer dict '"
    					+ path + "' : " + ioe.toString());
    		}
		}
		private void readTopicDict(String path) {
			try {
    			fis = new BufferedReader(new FileReader(path));
    			String line = null;
    			while ((line = fis.readLine()) != null) {
    				String[] tmp = line.split("\t");
    				topic_dict.put(tmp[1], Integer.parseInt(tmp[0]));
    			}
    		} catch (IOException ioe) {
    			System.err.println("Exception while reading topic dict  '"
    					+ path + "' : " + ioe.toString());
    		}
		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] tmp = value.toString().split("\t");
			if (answer_dict.containsKey(tmp[0]) == false) {
				return;
			}
			int an_id = answer_dict.get(tmp[0]);
			String[] topics = tmp[tmp.length-1].split(",");
			StringBuilder buffer = new StringBuilder();
			for (String topic : topics) {
				if (topic_dict.containsKey(topic) == false) {
					continue;
				}
				buffer.append(topic_dict.get(topic));
				buffer.append(',');
			}
			
			context.write(new IntWritable(an_id), new Text(buffer.toString()));
		}
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 * arg[0]: answer_infos.txt 3.3G
	 * arg[1]: answer_id.dict(cache)
	 * arg[2]: topic.dict(cache)
	 * arg[3]: output dir
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "Map answers to topics");
		job.setJarByClass(this.getClass());
		
		DistributedCache.addCacheFile(new Path(arg0[1]).toUri(), job.getConfiguration());
		DistributedCache.addCacheFile(new Path(arg0[2]).toUri(), job.getConfiguration());
		
		job.getConfiguration().set("answer_dict", arg0[1]);
		job.getConfiguration().set("topic_dict", arg0[2]);
		
		FileSystem fs=FileSystem.get(getConf());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[3]));
		
		
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setNumReduceTasks(1);
		/*
		if(fs.exists(new Path(arg0[1]))){
			fs.delete(new Path(arg0[1]),true);
		}
		*/
		int val= job.waitForCompletion(true) ? 0 : 1;
		
		return val;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Answer2Topic(), args);  
	  	  System.exit(exitCode);

	}

}
