import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AgeEffect {
	public static class Map extends  Mapper<LongWritable, Text, Text, Text> {
	    
	    Path[] cachefiles = new Path[0]; //To store the path of lookup files
        Map planedata = new HashMap();
	    
	    public boolean notEmpty( String ... values){
	        for (String st : values){
	             if (st.isEmpty())
	                return false;       
	        }
	       return  true;
	    }	
		
		
      public void setup(Context context) {
 
        Configuration conf = context.getConfiguration();
   		try 
   			{

  			cachefiles = DistributedCache.getLocalCacheFiles(conf);
  			BufferedReader reader = new BufferedReader(new FileReader(cachefiles[0].toString())); 
    		String line;

 			  while ((line = reader.readLine())!= null) 
  		      	{
  		      	    String[] tailYear =line.split(",");
   			    	planedata.put(tailYear[0],tailYear[8]);
   			    }
           
            }catch (IOException e){
              e.printStackTrace();
           }

     } 
		
		
	public void map(LongWritable key, Text value, Context context)
				throws IOException{
			
			String inputLine = value.toString();
        	String pattern = "(,\"[^\"]+),(.+\")";
   			String preprocessed = inputLine.replaceAll(pattern , "$1~$2");	   
    	    
    	    String[] tokens = preprocessed.split(",");	              
           
          	if (notEmpty(tokens[0], tokens[28],tokens[9]) && !tokens[0].startsWith("\"Year\"")){
               
                String delayIndicator = tokens[28] ;  // delay indicator
                String manufacturedYear = (String)planedata.get(tokens[9]); 
                String Pair = tokens[0] + "_" + manufacturedYear;
             
             context.write(new Text(Pair), new Text(delayIndicator) );			
		      
		    }
		   }
				
	}
	
	 
	public static class Combine extends  Reducer<Text, Text, Text, Text> {

  		public void reduce(Text key, Iterator<Text> values,
				Context context)
				throws IOException,InterruptedException {
			
			String sum_count;
			double sum = 0;
			int count = 0;
			while(values.hasNext()) {
				
				sum += Double.parseDouble(values.next().toString());
			    count += 1;	   
			 }
			sum_count = Double.toString(sum) + "_" + Integer.toString(count);
 			context.write(key, new Text(sum_count));
		}		
	
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterator<Text> values,
				Context context)
				throws IOException,InterruptedException {
			
			String[] pairs;
			double sum = 0;
			int count = 0;
			while(values.hasNext()) {
				pairs = values.next().toString().split("_");
				sum += Double.parseDouble(pairs[0]);
			    count += Double.parseDouble(pairs[1]);	   
			 }
			
			context.write(key, new DoubleWritable(sum/count));
		}		
	
	}
	
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
        	 job.setJarByClass(AgeEffect.class);
         	Job job = Job.getInstance(conf, "age effect");
    	 	job.setMapperClass(Map.class);
   		 job.setCombinerClass(Combine.class);
 		  job.setReducerClass(Reduce.class);
    	     job.setOutputKeyClass(Text.class);
   	     job.setOutputValueClass(DoubleWritable.class);
  	     DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
	     job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
			
  	     FileInputFormat.addInputPath(job, new Path(args[0]));
  	     FileOutputFormat.setOutputPath(job, new Path(args[1]));
   	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		
		}
}
