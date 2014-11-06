import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AverageCorrelate {
	
	
    
    public static class CorrMapper extends  Mapper<LongWritable, Text, Text, Text>{
           
            private static Text keyMR3 = new Text();
            private static Text valueMR3 = new Text();
            
	     	public void map(LongWritable key, Text line, Context context)
                throws IOException,InterruptedException {
            
             	String inputLine = line.toString();
            	String[] tokens = inputLine.split("\\s+"); // split on space	              
            	keyMR3.set(tokens[0]);	
            	valueMR3.set(tokens[1]);
             	context.write(keyMR3,valueMR3);
              }
            }
	
	public static class CorrReduce extends  Reducer<Text, Text, Text, DoubleWritable> {

         // rearrange the pairs so that carriers are together and averages are together
		public void reduce(Text key, Iterable<Text> values,Context context)
			 throws IOException,InterruptedException{
			 String[] pairs;
			 double x = 0.0;
 			 double y = 0.0;
			 double xx = 0.0;
			 double yy = 0.0;
			 double xy = 0.0;
			 double n = 0.0;
             
            try{ 
             for(Text txt : values){
                 			//while(values.hasNext()){ 
			     pairs  = txt.toString().split("\\|");
			     if (!pairs[0].isEmpty() && !pairs[1].isEmpty()){
				 x += Double.parseDouble(pairs[0]);
 				 y += Double.parseDouble(pairs[1]);
  				 xx += Math.pow(Double.parseDouble(pairs[0]), 2);
  				 yy += Math.pow(Double.parseDouble(pairs[1]), 2);
  				 xy += Double.parseDouble(pairs[0]) * Double.parseDouble(pairs[1]);
  			     n += 1.0;
  			     }
			}
		    double numerator = (n*xy) - (x * y);
 			double denominator1 = n*xx - Math.pow(x, 2);
		    double denominator2 = n*yy - Math.pow(y, 2);
 			double denominator = Math.sqrt(denominator1 * denominator2);
 			
 			double corr = numerator / denominator;
			context.write(key, new DoubleWritable(corr));

			}catch( Exception e){
			   e.printStackTrace();
			}
			} 
    }
    
 }