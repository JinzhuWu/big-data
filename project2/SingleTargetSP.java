package comp9313.ass2;


import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {
    // Set two Counters to judge the termination criterion.
	public static enum MyCounter {
		Changes,
		No_changes
	}

    public static String OUT = "output";
    public static String IN = "input";

    public static class STMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Mapper job for iterations
            StringTokenizer itr = new StringTokenizer(value.toString(), " |\t");
            int index = 0;
            while(itr.hasMoreTokens()) {
                index += 1;
                if(index == 1){
                    String SNode = itr.nextToken();
                    String Sdistance = itr.nextToken();
                    IntWritable Node = new IntWritable(Integer.parseInt(SNode));
                    double distance = Double.parseDouble(Sdistance);
                    Text new_dist = new Text();
                    String nodes_list = itr.nextToken();
                    String path = itr.nextToken();
                    if(!nodes_list.equals("NULL")){
                    // split by "-" to get adjacency lists
                    String[] aj_list = nodes_list.split("-");
                    for(int i=0;i<aj_list.length;i++){
                    	String[] tt = aj_list[i].split(":");
                    	IntWritable distance_node = new IntWritable(Integer.parseInt(tt[0]));
                    	
                    	double total_dist = Double.parseDouble(tt[1]) + distance;
                        // sum the distance from a adjacency node to node, and the distance from node to target node, then we get the new distance
                    	new_dist.set("Values "+ total_dist + " " + Node + " "+ path);
                    	context.write(distance_node, new_dist); 
                    	new_dist.clear();
                    	}
                    new_dist.set("Values " + distance + " "+ Node + " " + path);
                    context.write(Node, new_dist);
                	new_dist.set("Nodes "+ nodes_list + " " + path);
                	context.write(Node, new_dist);
                	new_dist.clear();
                    }
                    }
                    index = 0;}
            }

    }


    public static class STReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // reduce function for iterations
            // ... ...
        	Text word = new Text();
        	String node = new String();
        	double lowest = 10000;
        	String path = "Start:";
        	boolean isNode = false;
        	while(values.iterator().hasNext()){
        		
        		String[] sp = values.iterator().next().toString().split(" ");
                //distinguish the data is a distance or a record for nodes
        		if(sp[0].equalsIgnoreCase("Nodes")){
        			node = sp[1];   
        			isNode = true;
        		}
        		else if(sp[0].equalsIgnoreCase("Values")){
        			double distance = Double.parseDouble(sp[1]);
                    // comparing the new distance with old distance
        			if(lowest!=distance){        			
        			lowest = Math.min(distance, lowest); 
        			if(distance <= lowest){
        				int pre_node = Integer.parseInt(sp[2]);
            			path = sp[3];
            			path+= ("-" + pre_node);
        			}
        			if(distance>lowest){
            			context.getCounter(MyCounter.Changes).increment(1);
        			}
        			}else{
        				context.getCounter(MyCounter.No_changes).increment(1);
        				lowest = distance;
        			}
        		}       		
        	}
        	if(isNode==true){
        	word.set(lowest + "\t" + node + "\t" + path);}
        	else{
        		word.set(lowest + "\t" + "NULL" + "\t" + path);
        	}
        	path = "Start:";
        	context.write(key, word);
        	word.clear();
        }
    }

    public static class FirstMapper extends Mapper<Object, Text, IntWritable, Text> {
        Map<Integer, String> map = new HashMap<Integer, String>();
        @Override
        // First mapper job for transferring the input file to the redcuer
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " ");
            int index = 0;
            while(itr.hasMoreTokens()) {
                index += 1;
                if(index == 2){
                    String source = itr.nextToken();
                    String Starget = itr.nextToken();
                    int target = Integer.parseInt(Starget);
                    String dist = itr.nextToken();
                    // using ":" to create a adjacency list, the left side is adjacency node, the right side is distance
                    String added = source + ":" + dist;
                    if(map.containsKey(target)){
                        // using "-" to isolate different adjacency node
                    	String aj_list = map.get(target) + "-" + added;
                        map.put(target,aj_list);
                    }else{
                        map.put(target,added);
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
                Text tt = new Text(entry.getValue());
                context.write(new IntWritable(entry.getKey()), tt);
                }
                                                      }
                                                      }
    
    public static class FirstReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        
        @Override
        // First reducer job for converting the input file to the desired format for iteration
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // get args[2] as target node
            int single_target = Integer.parseInt(context.getConfiguration().get("single_target"));
        	IntWritable target_node = new IntWritable(single_target);
            // the distance from target node to target node is 0
            double target_dis = 0;
            // we assume Infinity as 10000, it is easier for comparison
            double lowest = 10000;
            Text new_list = new Text();
            // create a path record which is started with "Start:"
            Text path = new Text("Start:");
            while(values.iterator().hasNext()){
                String aj_list = values.iterator().next().toString();
                if(key.equals(target_node)){
                	new_list.set(target_dis + "\t" + aj_list);
                }
                else{
                	new_list.set(lowest + "\t" + aj_list);
                }
            }
            path.set(new_list + "\t" + "Start:");
            context.write(key , path);
        }
    }

    public static class FinalMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Final map function for transferring to final output format
            // ... ...
            StringTokenizer itr = new StringTokenizer(value.toString(), " |\t");
            int index = 0;
            Text word = new Text();
            while(itr.hasMoreTokens()) {
            	index+=1;
            if(index==1){
                String SNode = itr.nextToken();
                String Shortest_distance = itr.nextToken();
                IntWritable Node = new IntWritable(Integer.parseInt(SNode));
                double distance = Double.parseDouble(Shortest_distance);
                itr.nextToken();
                String path = itr.nextToken();
                word.set(distance + " " + path);
                context.write(Node , word);
            	}
            index = 0;
            }
            }
        }

    public static class FinalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Final reduce function for extracting the final result in correct output format
            // ... ...
        	Text word = new Text();
        	double inf = 10000;
        	int single_target = Integer.parseInt(context.getConfiguration().get("single_target"));
        	while(values.iterator().hasNext()){
        		String[] sp = values.iterator().next().toString().split(" ");
        		double distance = Double.parseDouble(sp[0]);
        		if(distance!=inf){
        		String[] xx = sp[1].split(":");
        		String[] path_list = xx[1].split("-");
        		int target = Integer.parseInt(path_list[1]);
        		if(target == single_target){
        			String path = key.toString();
        			boolean isexist = false;
        		for(int i=1;i<path_list.length;i++){
        			if(Integer.parseInt(path_list[i])==Integer.parseInt(key.toString())&isexist==false){
        				for(int j=i-1;j>=1;j--){
        					path = path + "->" + path_list[j];
        				}
        				isexist = true;
        			}
        		}
        		if(isexist==false){
        			for(int k=path_list.length-1;k>1;k--){
        				if(Integer.parseInt(path_list[k])!=Integer.parseInt(path_list[k-1])){
        					path = path + "->" + path_list[k];
        					
        				}
        				}
        			path = path + "->" + path_list[1];
        			}        			
        		word.set(distance + "\t" + path);
        		context.write(key, word);}
        		}
        		}
        }
        		    
    }

    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];

        int iteration = 0;

        String input = IN;

        String output = OUT + iteration;

	    // Configure and Convert the input file to the desired format for iteration
        // ... ...
        Configuration conf = new Configuration();
        conf.set("single_target",args[2]);
        Job job = Job.getInstance(conf, "SingleTargetSP");
        job.setJarByClass(SingleTargetSP.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);


        boolean isdone = false;
        // set two flags to compare with the values of counters in each iteration
        long flag1 =999;
        long flag2 =999;

        while (isdone == false) {

            // run the iterative MapReduce job
            // ... ...                   
            
            input = output;           

            iteration ++;

            output = OUT + iteration;
            
            Job job2 = Job.getInstance(conf, "SingleTargetSP");
            job2.setJarByClass(SingleTargetSP.class);
            job2.setMapperClass(STMapper.class);
            job2.setReducerClass(STReducer.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(input));
            FileOutputFormat.setOutputPath(job2, new Path(output));
            job2.waitForCompletion(true);
            
            Counters counters1 = job2.getCounters();
            org.apache.hadoop.mapreduce.Counter counter1 = counters1.findCounter(MyCounter.Changes);

            Counters counters2 = job2.getCounters();
            org.apache.hadoop.mapreduce.Counter counter2 = counters2.findCounter(MyCounter.No_changes);
            
            int deleted_index = iteration - 1;
            String deleted_path = OUT + deleted_index;
            FileSystem hdfs = FileSystem.get(URI.create(deleted_path),conf);
            Path deleteDir = new Path(deleted_path);
            hdfs.deleteOnExit(deleteDir);

            // Check the termination criterion by utilizing the counter, if the values of counters does not changes again, it means there is no update for output
            // ... ...
            if(counter1.getValue()==flag1&counter2.getValue()==flag2){
                isdone = true;
            }else{
                // assign the value of flag to previous value of counters
            	flag1 = counter1.getValue();
            	flag2 = counter2.getValue();
            }
       }
        input = output;
        // the final output file will be stored in "output"
        output = OUT;
        Job job3 = Job.getInstance(conf, "SingleTargetSP");
        job3.setJarByClass(SingleTargetSP.class);
        job3.setMapperClass(FinalMapper.class);
        job3.setReducerClass(FinalReducer.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(input));
        FileOutputFormat.setOutputPath(job3, new Path(output));
        job3.waitForCompletion(true);
        
        int deleted_index = iteration;
        String deleted_path = OUT + deleted_index;
        FileSystem hdfs = FileSystem.get(URI.create(deleted_path),conf);
        Path deleteDir = new Path(deleted_path);
        hdfs.deleteOnExit(deleteDir);
        
    }

}