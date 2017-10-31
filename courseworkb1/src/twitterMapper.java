import java.io.IOException;
import java.util.*;
import java.text.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class twitterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable hour = new IntWritable();
    private Text data = new Text();


    public void map(Object key, IntWritable value, Context context) throws IOException, InterruptedException {
     String[] fields = value.toString().split(";",-1);
	 if(fields.length == 4){
        	data.set(key.toString());
            	long epochtime = Long.parseLong(fields[0]);
            	Date date = new Date(epochtime);
            	SimpleDateFormat sdf = new SimpleDateFormat("HH", Locale.ENGLISH).format(date);
            	sdf.setTimeZone(TimeZone.getTimeZone("GMT-3"));
		int hourformatted = Integer.parseInt(sdf);
             	hour.set(hourformatted);
            context.write(data, hour);
          }
        }
      }
