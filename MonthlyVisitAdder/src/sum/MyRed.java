package sum;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyRed extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text k, Iterable <IntWritable> list, Context ctx) throws IOException, InterruptedException{
		
		int sum = 0;
		
		for (IntWritable v : list){
			
			sum = sum + v.get();
			
		}
		
		ctx.write(k, new IntWritable(sum));
		
	}

}