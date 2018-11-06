import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        // timing
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();

        /** Query 1 **/
        job.setMapperClass(Query1.QueryOneMapper.class);
        job.setReducerClass(Query1.QueryOneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        start = System.currentTimeMillis();
        job.waitForCompletion(true);
        end = System.currentTimeMillis();
        System.out.println("Query1 time taken : " + (end-start) + " ms");

        // read line count

        // print time

        /** Query 1 end **/

        /** Query 2 **/
        job.setMapperClass(Query2.QueryTwoMapper.class);
        job.setReducerClass(Query2.QueryTwoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        start = System.currentTimeMillis();
        job.waitForCompletion(true);
        end = System.currentTimeMillis();
        System.out.println("Query1 time taken : " + (end-start) + " ms");

        // read line count

        // print time

        /** Query 2 end **/

        /** Query 3 **/
        job.setMapperClass(Query3.QueryThreeMapper.class);
        job.setReducerClass(Query3.QueryThreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        start = System.currentTimeMillis();
        job.waitForCompletion(true);
        end = System.currentTimeMillis();
        System.out.println("Query1 time taken : " + (end-start) + " ms");

        // read line count

        // print time

        /** Query 3 end **/

        System.exit(0);
    }
}