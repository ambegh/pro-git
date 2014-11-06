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

public class AverageDelay {

  
  private static boolean isNotEmpty( String ... values){
	        for (String st : values){
	             if (st.isEmpty())
	                return false;       
	        }
	       return  true;
	    }	

  private static String trimQuotes(String value)
	{
		if ( value == null )
		return value;

		if (value.startsWith("\"")) value = value.substring(1,value.length());
		if (value.endsWith("\"")) value = value.substring(0,value.length() - 1);
		return value;
	}

  public static class DelayMapper extends  Mapper<LongWritable, Text, Text, Text>{
           
           	public static final int COL_DATE = 5;
			public static final int COL_CARRIER = 6;
			public static final int COL_DELAY_MINUTES = 26;

	     	public void map(LongWritable key, Text line, Context context)
                throws IOException,InterruptedException{
            
            double delayMin;  
        	String inputLine = line.toString();
            String pattern = "(,\"[^\"]+),([^\"]+\")";
   			String preprocessed = inputLine.replaceAll(pattern , "$1-$2");	 // replace , by -  
    	    
    	    String[] tokens = preprocessed.split(",");	              
    
           if (isNotEmpty(tokens[COL_CARRIER], tokens[COL_DATE], tokens[COL_DELAY_MINUTES])){
             // skip first line 
        	 if(! tokens[0].startsWith("\"Year\"") ){
            
                  tokens[COL_CARRIER]= trimQuotes(tokens[COL_CARRIER]);
                  tokens[COL_DATE] = trimQuotes(tokens[COL_DATE]);
                  tokens[COL_DELAY_MINUTES] = trimQuotes(tokens[COL_DELAY_MINUTES]);
                  
                  String carrier_date = tokens[COL_CARRIER]+ ":" + tokens[COL_DATE] ;
                  //delayMin = Double.parseDouble(tokens[COL_DELAY_MINUTES]);
                  context.write(new Text(carrier_date), new Text(tokens[COL_DELAY_MINUTES]) );			
		     
              }
            }
        }
   }

  public static class DelayCombiner extends Reducer<Text, Text, Text, Text> {
        
  		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException ,InterruptedException{
			
			String sum_count="";
			double sum = 0;
			int count = 0;
			for(Text txt : values){
			//while(values.hasNext()) {
				
					sum += Double.parseDouble(txt.toString());
					count += 1;	   
			 }
     		sum_count = Double.toString(sum) + "_" + Integer.toString(count);
 			context.write(key, new Text(sum_count));
		}		
	
	}
	
	public static class DelayReducer extends  Reducer<Text, Text, Text, DoubleWritable> {

        
		public void reduce(Text key, Iterable<Text> values,Context context)
			 throws IOException,InterruptedException {
			
			String[] pairs;
			double sum = 0;
			int count = 0;
			for(Text txt: values){
			//while(values.hasNext()) {
				pairs = txt.toString().split("_");
				sum += Double.parseDouble(pairs[0]);
			    count += Double.parseDouble(pairs[1]);	   
			 }
			
			context.write(key, new DoubleWritable(sum/count));
		}
		
		

 }
 }