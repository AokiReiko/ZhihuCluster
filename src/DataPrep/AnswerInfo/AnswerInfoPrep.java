import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnswerInfoPrep {

    public static class AnswerInfoPrepMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputVal = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString().trim();
            String[] info = str.split("\t");
            if (info.length != 18) return;
            outputKey.set(info[0]);
            outputVal.set(info[1] + "\t" + info[2] + "\t" + info[3] + "\t" + info[6] + "\t" + info[10] + "\t" + info[11] + "\t" + info[16] + "\t" + info[17]);
            context.write(outputKey, outputVal);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: AnswerInfoPrep <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Answer Info Prep");
        job.setJarByClass(AnswerInfoPrep.class);
        job.setMapperClass(AnswerInfoPrepMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}