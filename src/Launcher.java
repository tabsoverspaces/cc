import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Launcher {

    /**
     * The mapper will map each input as follows
     * key : term
     * value : map(article_id, score)
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text term = new Text();
        private Text article_id = new Text();

        private final String infantri = "infantry";
        private final String reinforc = "reinforc";
        private final String brigad = "brigad";
        private final String fire = "fire";


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] inputs  = line.split("\t");

            this.term.set(inputs[1]);
            this.article_id.set(inputs[0]);

            if(this.term.toString().equals(this.infantri)
            || this.term.toString().equals(this.reinforc)
            || this.term.toString().equals(this.brigad)
            || this.term.toString().equals(this.fire))
            {
                context.write(this.term, this.article_id);
            }
        }
    }

    /**
     * Reduce mapper results
     */
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Text sum = new Text();

            for (Text val : values) {

                sum = new Text(sum.toString()+val.toString());

            }

            result.set(sum);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}