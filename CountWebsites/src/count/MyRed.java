package count;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyRed extends Reducer<Text, Text, Text, IntWritable> {
	
	// K			V
	// distSites	{"univ.it", "science.edu" ... }
	public void reduce(Text k, Iterable <Text> list, Context ctx) throws IOException, InterruptedException{
		
		ArrayList<String> distSites = new ArrayList<String>();
		
		int count = 0;
		for (Text v : list){
			if(!distSites.contains(v.toString())){
				distSites.add(v.toString());
				count++;
			}
		}
		
		ctx.write(new Text("distSites"), new IntWritable(count));
		
	}

}
