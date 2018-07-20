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

public class UserInfoPrep {

    public static class UserInfoPrepMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputVal = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString().trim();
            String[] info = str.split("\t");
            if (info.length != 27) return;
            outputKey.set(info[0]);
            outputVal.set(info[10] + "\t" + info[11] + "\t" + info[25] + "\t" + info[5] + "\t" + info[26]);
            context.write(outputKey, outputVal);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: UserInfoPrep <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "User Info Prep");
        job.setJarByClass(UserInfoPrep.class);
        job.setMapperClass(UserInfoPrepMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}