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

    static final String infantri = "infantri";
    static final String reinforc = "reinforc";
    static  final String brigad = "brigad";
    static  final String fire = "fire";

    static boolean containsTerm(String[] terms, String term)
    {
        for(int i = 0 ; i< terms.length;i++)
            if(terms[i].equals(term))
                return true;

        return false;
    }

    /**
     * The mapper will map each input as follows
     * key : term
     * value : map(article_id, score)
     */
    public static class QueryOneMapper extends Mapper<Object, Text, Text, Text> {

        private Text term = new Text();
        private Text article_id = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] inputs  = line.split("\t");

            this.term.set(inputs[1]);
            this.article_id.set(inputs[0]);

            if(this.term.toString().equals(infantri)
            || this.term.toString().equals(reinforc)
            || this.term.toString().equals(brigad)
            || this.term.toString().equals(fire))
            {
                context.write(this.article_id, this.term);
            }
        }
    }

    /**
     * Reduce mapper results
     * Query 1
     *
     * any term
     */
    public static class QueryOneReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Text sum = new Text();

            for (Text val : values) {

                sum = new Text(sum.toString()+","+val.toString());
            }

            // check the sum , if fulfills condition, submit
//            String[] terms = sum.toString().split(",");
//
//            if(containsTerm(terms, infantri) ||
//            containsTerm(terms, reinforc) ||
//                    containsTerm(terms, brigad) ||
//                            containsTerm(terms,fire))
//            {
//
//            }


            context.write(key,sum);
        }



    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        job.setMapperClass(QueryOneMapper.class);
        // job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
        job.setReducerClass(QueryOneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}