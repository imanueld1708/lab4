import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import java.text.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class twitterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {
       	for(IntWritable value : values){
           result.set(values);
        }
        context.write(key,result);
    }

}
