package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
 * Version2: EdgeAvgLen2 -- which is in-mapper-conbining design pattern
 * Including TokenizerMapper(which use hashmap to store key-value) and IntSumReducer class(which is actually calculated for the average length, I'm lazy to change the class name)
 * Firstly, we also need to implement a custom class containing a pair
 */

public class EdgeAvgLen2 {
    
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
     * It implements the map method of Mapper class and gets the incoming nodes and their length, which are the third Token and fourth Token. I use 'index' to locate the useful data, and ":" character to split single length and count once, and then package them as value in map. Traversing the map and sum the total distance for same key and the number of key, finally let 'index = 0' to return to every line and write key-value into context.
     */

	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Pair> {
        Map<Integer, String> map = new HashMap<Integer, String>();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
            int index = 0;
			while (itr.hasMoreTokens()) {
                index += 1;
                if(index == 3){
                    String Snode = itr.nextToken();
                    String dist = itr.nextToken();
                    double distance = Double.parseDouble(dist);
                    int node = Integer.parseInt(Snode);
                    if(!map.containsKey(node)){
                        map.put(node, distance+":"+1);}
                    else{
                        String tmp = map.get(node);
                        String[] tt = tmp.split(":");
                        double t_left = distance + Double.parseDouble(tt[0]);
                        double t_right = Double.parseDouble(tt[1])+1;
                        map.put(node, t_left+":"+t_right);
                    }
                    index = 0;}
                             else{
                                 itr.nextToken();
                             }
                             }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<Integer, String> > sets = map.entrySet();
            for(Entry<Integer, String> entry: sets){
                    String[] tt = entry.getValue().split(":");
                    double t_first = Double.parseDouble(tt[0]);
                    double t = Double.parseDouble(tt[1]);
                    int t_second = (int)(t);
                    Pair p = new Pair(t_first, t_second);
                    context.write(new IntWritable(entry.getKey()), p);
                }
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
		Job job = Job.getInstance(conf, "Edge Avg Length 2");
		job.setJarByClass(EdgeAvgLen2.class);
		job.setMapperClass(TokenizerMapper.class);
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
