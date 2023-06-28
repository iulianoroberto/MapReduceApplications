package count;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MyMap extends Mapper<LongWritable, Text, Text, Text>{
	
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
			
			ctx.write(new Text("sites"), new Text(webSite));
			
		}
		
		// K				V
		// distsites 		univ.it
		
	}

}

