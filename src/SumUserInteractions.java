import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class SumUserInteractions extends Configured implements Tool {

	public SumUserInteractions() {
		// TODO Auto-generated constructor stub
	}

	public SumUserInteractions(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	public static class Map extends Mapper<Text, Text, IntWritable, Text>{
		
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int num = value.toString().split(",").length;
			context.write(new IntWritable(num), key);
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "Sum up user interactions");
		job.setJarByClass(this.getClass());
		
		FileSystem fs=FileSystem.get(getConf());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setNumReduceTasks(1);
		if(fs.exists(new Path(arg0[1]))){
			//fs.delete(new Path(arg0[1]),true);
		}
		int val= job.waitForCompletion(true) ? 0 : 1;
		
		return val;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SumUserInteractions(), args);  
	  	  System.exit(exitCode);

	}

}
