package bdp.stock;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StockJoinMapper extends Mapper<Text, DailyStock, TextIntPair, LongWritable> {
    //private final Do one = new DoubleWritable(1);
    private Text data = new Text();
    private TextIntPair year = new TextIntPair();
    private DoubleWritable low = new DoubleWritable();

    public void map(TextIntPair key, Text value, Context context) throws IOException, InterruptedException {

        context.write(value.getCompany(), value.getHigh());
        context.write(value.getCompany(), value.getLow());
        }
    }
