package min;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyRed extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text k, Iterable <IntWritable> list, Context ctx) throws IOException, InterruptedException{
		
		int min = Integer.MAX_VALUE;
		
		for (IntWritable v : list){
			if(v.get() < min){
				min = v.get();
			}
		}
		
		ctx.write(new Text("minVisits"), new IntWritable(min));
		
	}

}