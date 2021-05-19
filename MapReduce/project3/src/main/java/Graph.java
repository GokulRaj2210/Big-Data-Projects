import java.io.*;
import java.lang.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                
    public int distance;              

    public Vector<Integer> following;  

    Tagged () { tag = false; }
    Tagged ( int d ) { tag = false; distance = d; }
    Tagged ( int d, Vector<Integer> f ) { tag = true; distance = d; following = f; }

    public void write ( DataOutput out ) throws IOException {
        
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (tag) {
            out.writeInt(following.size());
            
            for ( int i = 0; i < following.size(); i++ ){
                
                out.writeInt(following.get(i));

            }
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        
      
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for ( int i = 0; i < n; i++ )
                following.add(in.readInt());

                  
      
        }
    }
}

public class Graph {
                                 
   
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

 



     public static class firstMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
            Scanner s=new Scanner(value.toString()).useDelimiter(",");
            int x=s.nextInt();
            int y=s.nextInt();
                   

            context.write(new IntWritable(y),new IntWritable(x));
            s.close();
        }
    }

public static class Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, Tagged> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           
            Vector<Integer> following = new Vector<Integer>();
            
            for(IntWritable v : values) {
                String s=v.toString(); 
                int intv=Integer.parseInt(s); 
                    following.addElement(intv);
            }
                    

            String str=key.toString();
            int intkey=Integer.parseInt(str); 

           

            if(intkey==start_id || intkey==1)
                context.write(key,new Tagged(0,following));

            else
                context.write(key,new Tagged(max_int,following));

        
        }
    }


     public static class secondMapper extends Mapper<IntWritable, Tagged, IntWritable, Tagged> {
        @Override
        public void map(IntWritable key, Tagged value, Context context) throws IOException, InterruptedException {
            context.write(key,new Tagged( value.distance, value.following));
                               
             if(value.distance<max_int)
            for(int id:value.following){
               
                context.write(new IntWritable(id), new Tagged((value.distance)+1));
            }

        }
    }






  public static class Reducer2 extends Reducer<IntWritable, Tagged, IntWritable, Tagged> {
        @Override
        public void reduce(IntWritable key, Iterable<Tagged> values, Context context) throws IOException, InterruptedException {
            int  m = max_int;
             Vector<Integer> followingvect = new Vector<Integer>();
                 

            for(Tagged v : values) {
                if (v.distance<m) {
                    m=v.distance;
                }
                if (v.tag) {
                    followingvect = v.following;
                    
                }
            }
            
            context.write(key, new Tagged(m,followingvect));
        }
    }




     public static class thirdMapper extends Mapper<IntWritable, Tagged, IntWritable, IntWritable> {
        @Override
        public void map(IntWritable key, Tagged value, Context context) throws IOException, InterruptedException {
                             

            if(value.distance<max_int)
            context.write(key, new IntWritable(value.distance));
        }
    }

      




    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        
     

         Job j1 = Job.getInstance();
        j1.setJobName("MyJob");
        j1.setJarByClass(Graph.class);
        j1.setMapperClass(firstMapper.class);
        j1.setReducerClass(Reducer1.class);
        j1.setOutputKeyClass(IntWritable.class);
        j1.setOutputValueClass(Tagged.class);
        j1.setMapOutputKeyClass(IntWritable.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path( args[1]+"0"));
        j1.waitForCompletion(true);



        for ( short i = 0; i < iterations; i++ ) {
            
            
            Job j2= Job.getInstance();
            j2.setJobName("MyJob2");
            j2.setJarByClass(Graph.class);
            j2.setMapperClass(secondMapper.class);
            j2.setReducerClass(Reducer2.class);
            j2.setOutputKeyClass(IntWritable.class);
            j2.setOutputValueClass(Tagged.class);
            j2.setMapOutputKeyClass(IntWritable.class);
            j2.setMapOutputValueClass(Tagged.class);
            j2.setInputFormatClass(SequenceFileInputFormat.class);
            j2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(j2,new Path(args[1]+i));
            FileOutputFormat.setOutputPath(j2,new Path(args[1]+(i+1)));
            j2.waitForCompletion(true);
           
        
        }


        Job j3= Job.getInstance();
        j3.setJobName("MyJob3");
        j3.setJarByClass(Graph.class);
        j3.setMapperClass(thirdMapper.class);
        j3.setNumReduceTasks(0);
       j3.setOutputKeyClass(IntWritable.class);
        j3.setOutputValueClass(IntWritable.class);
        j3.setMapOutputKeyClass(IntWritable.class);
        j3.setMapOutputValueClass(IntWritable.class);
        j3.setInputFormatClass(SequenceFileInputFormat.class);
        j3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(j3,new Path(args[1]+iterations));
        FileOutputFormat.setOutputPath(j3,new Path(args[2]));
        j3.waitForCompletion(true);
        
       
    }
}


