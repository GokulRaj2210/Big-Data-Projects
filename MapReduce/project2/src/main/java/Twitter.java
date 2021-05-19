import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Twitter {
   

   
   
     class firstMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
            Scanner s=new Scanner(value.toString()).useDelimiter(",");
            int x=s.nextInt();
            int y=s.nextInt();
            context.write(new IntWritable(y),new IntWritable(x));
            s.close();
        }
    }

     


   class firstCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        public void reduce ( IntWritable key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException {
            
            int count = 0;
            for (IntWritable v: values ) {
                
                count++;
            };
            context. write (key,new IntWritable(count));
        } 
    }

    class secondMapper extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable>{
        
        Map uid = new HashMap<Integer, Integer>();
		Map  followingcount  = new HashMap<Integer, Integer>();

        public void map(IntWritable key,IntWritable value, Context context) throws IOException, InterruptedException{
           
            			
			if (uid.containsKey(key)) {
                
				uid.put(key, value);
			} else {
				uid.put(value, key);
			}
			
			if (followingcount.containsKey(value)) {
                String s=followingcount.get(key).toString();
                int prev=Integer.parseInt(s);  
				int count =  prev+  1;
				followingcount.put(value, count);
			} else {
				followingcount.put(value, 1);
			}
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
			
            Iterator<Map.Entry<Integer, Integer>> itr1 = uid.entrySet().iterator();
			
			while (itr1.hasNext()) {
				Entry<Integer, Integer> entry1 = itr1.next();
				Set followingcount_set = followingcount.entrySet();
				Integer key_1 = entry1.getKey();
				Integer uid_1 = entry1.getValue();
				Integer followingcount_1 = (Integer) followingcount.get(key_1);

				context.write(new IntWritable(key_1), new IntWritable(followingcount_1));
				
			}
		}
    }
     class firstReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        @Override
        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) 
            throws IOException, InterruptedException{
             int count=0;
            for(IntWritable v:values){
                count++;
            }; 
           
            context. write (key,new IntWritable(count));
        }
    }
    public static class secondReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            
            Integer fc = key.get();
			Integer sum = 0;
			Integer cnt = 0;
			
			for (IntWritable value:values) {
				
				cnt ++;
			}
			
			
			context.write(new IntWritable(fc), new IntWritable(cnt));

        }
    }

    public static void main ( String[] args ) throws Exception {
        Job j1=Job.getInstance();
        j1.setJobName("MyJob");
        j1.setJarByClass(Twitter.class);
        j1.setOutputKeyClass(IntWritable.class);
        j1.setOutputValueClass(IntWritable.class);
        j1.setMapOutputKeyClass(IntWritable.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setMapperClass(firstMapper.class);
        j1.setCombinerClass(firstCombiner.class);
        j1.setReducerClass(firstReducer.class);
        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        j1.waitForCompletion(true);

        Job j2=Job.getInstance();
        j2.setJobName("MyJob2");
        j2.setJarByClass(Twitter.class);
        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(IntWritable.class);
        j2.setMapOutputKeyClass(IntWritable.class);
        j2.setMapOutputValueClass(IntWritable.class);
        j2.setMapperClass(secondMapper.class);
        j2.setReducerClass(secondReducer.class);
        j2.setInputFormatClass(SequenceFileInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(j2,new Path(args[1]));
        FileOutputFormat.setOutputPath(j2,new Path(args[2]));
        j2.waitForCompletion(true);
    }
}


