package bdp.stock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StockJoinReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
              throws IOException, InterruptedException {

        double min = Integer.MAX_VALUE;
        double max = Integer.MIN_VALUE;

        for (DoubleWritable value : values) {
          if(min > value.get())
          min = value.get();
          if(max < value.get())
          max = value.get();
        }

        String print = "MIN: " + min + "  MAX: " + max;
        result.set(print);
        context.write(key,result);

    }

}
