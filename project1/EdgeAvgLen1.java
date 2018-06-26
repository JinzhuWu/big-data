package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Version1: EdgeAvgLen1
 * Including TokenizerMapper, MyCombiner and IntSumReducer class(which is actually calculated for the average length, I'm lazy to change the class name)
 * Firstly, we need to implement a custom class containing a pair
*/

public class EdgeAvgLen1 {
    
    public static class Pair implements Writable{
        private double first;
        private int second;
        
        public Pair(){
        }
        public Pair(double first, int second) {
            set(first,second);
        }
        public void set(double left, int right) {
            first = left;
            second = right;
        }
        public double getFirst(){
        	return first;
        	}
        public int getSecond(){
        	return second;
        	}
        
        public void readFields(DataInput in) throws IOException{
        	first = in.readDouble();
        	second = in.readInt();
        	}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(first);
        	out.writeInt(second);
			// TODO Auto-generated method stub
			
		}
    }
        
/**
 * It implements the map method of Mapper class and gets the incoming nodes and their length, which are the third Token and fourth Token. I use 'index' to locate the useful data, 'pair' to store single length and count once, and then write into context, finally let 'index = 0' to return to every line.
*/
    
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Pair> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
            int index = 0;
			while (itr.hasMoreTokens()) {
                index = index + 1;
                if(index == 3){
                	String Snode = itr.nextToken();
                    String dist = itr.nextToken();
				double distance = Double.parseDouble(dist);
                    IntWritable node = new IntWritable(Integer.parseInt(Snode));
                    context.write(node, new Pair(distance,1));
                    index = 0;
                    }
                             
                 else{
                                 itr.nextToken();
                             }    
                             }
                             }

			}
    
    /**
     * It implements the Combiner class and the input value is a pair. Using get() to take the first data and second data in pair, and then count them as new inputs for Reducer.
     */

    public static class MyCombiner extends Reducer<IntWritable, Pair, IntWritable, Pair>{
        protected void reduce(IntWritable key, Iterable<Pair> value,
                              Context context)
        throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            while(value.iterator().hasNext()){
                Pair p = value.iterator().next();
                sum += p.getFirst();
                count += p.getSecond();
            }
            context.write(key, new Pair(sum,count));
        }      
    }
    
    /**
     * It implements the reduce method of Reducer class and sum length for the same key. Using sum/count to calculate avgDistance.
     */
    
	public static class IntSumReducer extends Reducer<IntWritable, Pair, IntWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<Pair> value, Context context)
				throws IOException, InterruptedException {
                    double sum = 0.0;
			int count = 0;
            while(value.iterator().hasNext()){
                Pair p = value.iterator().next();
                sum += p.getFirst();
                count += p.getSecond();
            }
            DoubleWritable avgDistance = new DoubleWritable(sum/count);
			context.write(key, avgDistance);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Edge Avg Length 1");
		job.setJarByClass(EdgeAvgLen1.class);
		job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Pair.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
