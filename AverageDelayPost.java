import java.io.IOException;
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


public class AverageDelayPost {

  
   public static class DelayMapperPost extends Mapper<LongWritable, Text, Text, Text>{
           
            private static Text keyMR2 = new Text();
            private static Text valueMR2 = new Text();
            
	     	public void map(LongWritable key, Text line, Context context)
                throws IOException,InterruptedException {
            	String bind ="";
           	 	String inputLine = line.toString();
            	String[] tokens = inputLine.split("\\s+"); // split on space	              
            		
            	String[] keyPairFromMR1 = tokens[0].split(":",2); // split carrier and date from first pair
             
             	keyMR2.set(keyPairFromMR1[1]); // extract date :group by date as key 
             	bind = keyPairFromMR1[0] +"_"+tokens[1]; // combine carrier and avg
             	valueMR2.set(bind);
             	context.write(keyMR2,valueMR2);
              }
            }
    
    public static class DelayReducerPost extends  Reducer<Text, Text, Text, Text> {

        private Text CarrierPair = new Text();
		private Text AveragePair = new Text();
        
        // rearrange the pairs so that carriers are together and averages are together
		public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException,InterruptedException {
			
			String carrier12, avg12; 
			String[] carrier_avg,carrier_avg2;
			
			Iterable<Text> innerloop =values;
			
		    for(Text txt: values)
		      {
		         carrier_avg = txt.toString().split("_");
		         //if (carrier_avg[0].equals("WN")){ // choose carrier to compare against all others
		            for(Text in: innerloop){
		           // while( innerloop.hasNext()){
		                   carrier_avg2 = in.toString().split("_");
		                   carrier12 = carrier_avg[0] +"_"+ carrier_avg2[0];
		                   avg12 = carrier_avg[1]+"|"+carrier_avg2[1]; 
		                   CarrierPair.set(carrier12);
		                   AveragePair.set(avg12);           
		                   context.write(CarrierPair,AveragePair);
		            }
		         //}
		    
		    }
			
	 } 
    }
 }