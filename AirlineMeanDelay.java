import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class trial {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			
			String inputLine = value.toString();
            String pattern = "(,\"[^\"]+),(.+\")"; // some of the field values have a , in side "" which disturbs the splitting 
            
   			String preprocessed = inputLine.replaceAll(pattern , "$1~$2");	   // replace , with ~ if it found in side double quotes
    	    
    	    String[] tokens = preprocessed.split(",");	              
            String airline_month = tokens[2] + "_" + tokens[6] ;
            double delayMin;  
        	
        	if (! tokens[0].startsWith("\"Year\"")){	
			  if ( ! tokens[27].isEmpty()){
			  delayMin = Double.parseDouble(tokens[27]);
			  output.collect(new Text(airline_month), new DoubleWritable(delayMin) );			
		     }
		    }
		}		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			
			
			double sum = 0;
			int count = 0;
			while(values.hasNext()) {
				
				sum += values.next().get();
			    count += 1;	   
			 }
			
			output.collect(key, new DoubleWritable(sum/count));
		}		
	
	}
	
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(trial.class);
		conf.setJobName("mean");
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.set("key.value.separator.in.input.line", " ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	      
		JobClient.runJob(conf);
	}
}
