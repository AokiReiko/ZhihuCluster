import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DeduplicateUserInteraction extends Configured implements Tool {

	public DeduplicateUserInteraction() {
		// TODO Auto-generated constructor stub
	}

	public DeduplicateUserInteraction(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	/*
	 * userID \t answerID(long) \t numOfInteractions \t listOFAnswerID(short)
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tmp = value.toString().split("\t");
			Text user = new Text(tmp[0]);
			if (Integer.parseInt(tmp[2]) == 0) {
				return;
			}
			for (String idStr: tmp[3].split(",")) {
				context.write(user, new IntWritable(Integer.parseInt(idStr)));
			}
		}
	}
	public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text user, Iterable<IntWritable> list, Context context)
				throws IOException, InterruptedException {	
			Set<Integer> userInteractions = new HashSet<>();
			for (IntWritable a_id : list) {
				if (userInteractions.contains(a_id.get())) {
					continue;
				}
				context.write(user, a_id);
				userInteractions.add(a_id.get());
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text>{
		@Override
		public void reduce(Text user, Iterable<IntWritable> list, Context context)
				throws IOException, InterruptedException {	
			Set<Integer> userInteractions = new HashSet<>();
			StringBuilder buffer = new StringBuilder();
			for (IntWritable a_id : list) {
				if (userInteractions.contains(a_id.get())) {
					continue;
				}
				buffer.append(String.valueOf(a_id.get()));
				buffer.append(',');
				userInteractions.add(a_id.get());
			}
			context.write(user, new Text(buffer.toString()));
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "Deduplicate user interactions");
		job.setJarByClass(this.getClass());
		
		FileSystem fs=FileSystem.get(getConf());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//job.setNumReduceTasks(1);
		if(fs.exists(new Path(arg0[1]))){
			//fs.delete(new Path(arg0[1]),true);
		}
		int val= job.waitForCompletion(true) ? 0 : 1;
		
		return val;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DeduplicateUserInteraction(), args);  
	  	  System.exit(exitCode);

	}

}
