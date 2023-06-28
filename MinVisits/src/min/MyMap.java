package min;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	
public void map(LongWritable l, Text t, Context ctx) throws IOException, InterruptedException{
		
		String s = t.toString();
		StringTokenizer st = new StringTokenizer(s);
		
		while(st.hasMoreTokens()){
			
			String month = st.nextToken();
			String webSite = st.nextToken();
			int numberOfVisits = Integer.parseInt(st.nextToken());
			
			System.out.println(month);
			System.out.println(webSite);
			System.out.println(numberOfVisits);
			
			ctx.write(new Text("visits"), new IntWritable(numberOfVisits));
			
		}
		
	}

}
