import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class TrainingSetPrep {

	public static class TrainingSetPrepMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputVal = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString().trim();
			String[] info = str.split("\t");
			if (info.length != 8) {
				return;
			}
			String[] docs = info[2].split(",");
			int n = Integer.valueOf(info[1]);
			if (docs.length != n) {
				return;
			}
			int a = 0;
			boolean first = true;
			StringBuilder sBuilder = new StringBuilder();
			for (int i = 0; i < docs.length; i ++) {
				if (docs[i].startsWith("Q")) {
					continue;
				} else if (docs[i].startsWith("A")) {
					a ++;
				} else {
					continue;
				}
				if (!first) {
					sBuilder.append(",");
				} else {
					first = false;
				}
				int pos = docs[i].indexOf("|");
				sBuilder.append(docs[i].substring(1, pos));
			}

			outputKey.set(info[0]);
			outputVal.set(info[7] + "\t" + a + "\t" + sBuilder.toString());
			context.write(outputKey, outputVal);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TrainingSetPrepPrep <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Training Set Prep");
		job.setJarByClass(TrainingSetPrep.class);
		job.setMapperClass(TrainingSetPrepMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}