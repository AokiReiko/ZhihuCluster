import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Sample extends Configured implements Tool {

	public Sample() {
		// TODO Auto-generated constructor stub
	}
	public static class SumMap extends Mapper<Text, Text, IntWritable, Text>{
		@Override
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			String[] topics = value.toString().split(",");
			int sum = 0;
			for (String topic: topics) {
				String[] tmp = topic.split(" ");
				if (tmp.length < 2) {
					continue;
				}
				sum += Integer.parseInt(tmp[1]);
			}
			context.write(new IntWritable(sum), user);
		}
	}
	public static class PickMap extends Mapper<Text, Text, Text, Text>{
		private HashSet<String> users = new HashSet<>();
		private BufferedReader fis;
		@Override
	    public void setup(Context context) throws IOException,
	        InterruptedException {
			Configuration conf = context.getConfiguration();
			String path = context.getLocalCacheFiles()[0].toString();//conf.get("answer2topic");
			readUsers(path);
	    }
		private void readUsers(String path) throws IOException {
			fis = new BufferedReader(new FileReader(path));
			String line = null;
			while ((line = fis.readLine()) != null) {
				String[] tmp = line.split("\t");
				users.add(tmp[1]);
			}	
	}
		
		public void map(Text user, Text value, Context context)
				throws IOException, InterruptedException {
			if (users.contains(user.toString())) {
				context.write(user, value);
			}
			
		}
	}
	public static class IntComparator extends WritableComparator {
		  public IntComparator() {
		    super(IntWritable.class);
		  }

		  @Override
		  public int compare(byte[] b1, int s1, int l1, byte[] b2,
		        int s2, int l2) {
		    Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
		    Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
		    return v1.compareTo(v2) * (-1);
		  }
		}
	public static class SumReduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		private int counter = 0;
		private int N = 1000;
		@Override
		public void reduce(IntWritable num, Iterable<Text> users, Context context)
				throws IOException, InterruptedException {
			for (Text user : users) {
				counter += 1;
				if (counter > N) {
					return;
				}
				context.write(num, user);
			}
		}
	}
	/**
	 * arg[0]: user2Topics
	 * arg[1]: output
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		String tmpdir = "tmp";
		String vecdir = "normalizedUserVec";
		Job job = Job.getInstance(getConf(), "Pick Most active users");
		job.setJarByClass(this.getClass());
		
		FileSystem fs=FileSystem.get(getConf());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(tmpdir));
		
		
		job.setMapperClass(SumMap.class);
		job.setReducerClass(SumReduce.class);
		job.setSortComparatorClass(IntComparator.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		int val = 0;
		//job.setNumReduceTasks(1);
		if(fs.exists(new Path(tmpdir)) == false){
			//fs.delete(new Path(arg0[1]),true);
			val= job.waitForCompletion(true) ? 0 : 1;
		}
		
		if (val == 1) return val;
		
		job = Job.getInstance(getConf(), "Output active users");
		job.setJarByClass(this.getClass());
		
		DistributedCache.addCacheFile(new Path(tmpdir+"/part-r-00000").toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		
		FileInputFormat.addInputPath(job, new Path(vecdir));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		job.setMapperClass(PickMap.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		val= job.waitForCompletion(true) ? 0 : 1;
		
		return val;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Sample(), args);  
	  	  System.exit(exitCode);

	}

}
