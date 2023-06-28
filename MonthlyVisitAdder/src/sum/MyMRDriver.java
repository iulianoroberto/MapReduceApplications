package sum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;

public class MyMRDriver {

	public static void main(String[] args) throws Exception {
		
		Job j = Job.getInstance();
		j.setJarByClass(sum.MyMRDriver.class);
		j.setJobName("MonthlyVisitAdder");
		j.setNumReduceTasks(6);

		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		
		j.setMapperClass(sum.MyMap.class);
		j.setReducerClass(sum.MyRed.class);
	
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		System.exit(j.waitForCompletion(true) ? 0:1);

	}

}