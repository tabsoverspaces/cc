import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Find URLs of Wikipedia articles that contain the stemmed keyword reinforc
but not any of the stemmed keywords infantri, brigad, or fire.

 */
public class Query3 {

    /**
     * Query2 mapper
     */
    public static class QueryThreeMapper extends Mapper<Object, Text, Text, Text> {

        private Text term = new Text();
        private Text article_id = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] inputs  = line.split("\t");

            this.term.set(inputs[1]);
            this.article_id.set(inputs[0]);

            if(this.term.toString().equals(Launcher.infantri)
            || this.term.toString().equals(Launcher.reinforc)
            || this.term.toString().equals(Launcher.brigad)
            || this.term.toString().equals(Launcher.fire))
            {
                context.write(this.article_id, this.term);
            }
        }
    }

    /**
     * Query 2 reducer
     */
    public static class QueryThreeReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Text sum = new Text();

            for (Text val : values) {

                sum = new Text(sum.toString()+" "+val.toString());
            }

            // check the sum , if fulfills condition, submit
            String[] terms = sum.toString().split(",");

            if(Launcher.containsTerm(terms, Launcher.reinforc) &&
                    (
            !(Launcher.containsTerm(terms, Launcher.infantri)) &&
                    !(Launcher.containsTerm(terms, Launcher.brigad)) &&
                            !(Launcher.containsTerm(terms,Launcher.fire))))
            {

            }

            context.write(key,sum);
        }

    }
}
