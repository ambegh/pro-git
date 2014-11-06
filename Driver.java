
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
//import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Driver 
{

	
	public static void main(String[] args) throws IOException {
	
    try{
    
    //Configuration conf = new Configuration();
    Job job1 = new Job();
    //JobConf jconf1 = new JobConf(conf, AverageDelay.class);
    job1.setJarByClass(AverageDelay.class);
    
    job1.setMapperClass(AverageDelay.DelayMapper.class);
    job1.setCombinerClass(AverageDelay.DelayCombiner.class);
    job1.setReducerClass(AverageDelay.DelayReducer.class);
   
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
		
 	job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(DoubleWritable.class);
    //Job job1 = new Job(jconf1);
   
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    //JobClient.runJob(jconf1);
    job1.waitForCompletion(true);
   
    Job job2 = new Job();
    //JobConf jconf1 = new JobConf(conf, AverageDelay.class);
    job2.setJarByClass(AverageDelay.class);
    
    job2.setMapperClass(AverageDelayPost.DelayMapperPost.class);
   // job2.setCombinerClass(AverageDelayPost.DelayReducerPost.class);
    job2.setReducerClass(AverageDelayPost.DelayReducerPost.class);
   
    //job2.setMapOutputKeyClass(Text.class);
    //job2.setMapOutputValueClass(Text.class);
		
 	job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    //Job job1 = new Job(jconf1);
   
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    //JobClient.runJob(jconf1);
    job2.waitForCompletion(true);
   
    Job job3 = new Job();
    //JobConf jconf1 = new JobConf(conf, AverageDelay.class);
    job3.setJarByClass(AverageDelay.class);
    
    job3.setMapperClass(AverageCorrelate.CorrMapper.class);
    job3.setReducerClass(AverageCorrelate.CorrReduce.class);
   
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);
		
 	job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(DoubleWritable.class);
    //Job job1 = new Job(jconf1);
   
    FileInputFormat.addInputPath(job3, new Path(args[2]));
    FileOutputFormat.setOutputPath(job3, new Path(args[3]));
    //JobClient.runJob(jconf1);
    job3.waitForCompletion(true);
   
   
  
   }catch ( Exception e){
       System.out.print("Exception :" + e.getMessage());   
   		e.printStackTrace();
   }
 	
 }
}
