/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nbayes_mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author deepak
 */
public class NBAYES_MR {

    /**
     * @param args the command line arguments
     */
	public static Map<String,Integer> probxmap=new HashMap<String,Integer>();
	public static Map<String,Integer> probxhmap=new HashMap<String,Integer>();
	public static Map<String,Integer> probhmap=new HashMap<String,Integer>();
	public static int total;
	public static ArrayList<Double> accuracy=new ArrayList<Double>();
	
	
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            //System.out.println(value.toString());
            for (int i = 0; i < split.length; i++) {
                if (i != 10) {
                	context.write(new Text("X:"+split[i]), new Text("1"));
                    context.write(new Text("X|H:"+split[i] + "|" + split[10]), new Text("1"));
                }
                if(i==10){
                	context.write(new Text("H:"+split[i]), new Text("1"));
                }
            }
            context.write(new Text("Total"), new Text("1"));
        }
    }

	public static class IntSumCombiner extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Integer sum = 0;
    for (Text val : values) {
        sum += Integer.valueOf(val.toString());
    }
    try {
        context.write(new Text(key.toString()), new Text(String.valueOf(sum)));
    } catch (IOException e) {
        e.printStackTrace();
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
}
}


	
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
       public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            for (Text val : values) {
                sum += Integer.valueOf(val.toString());
            }
            try {
                context.write(new Text(key.toString()+"_"+String.valueOf(sum)), new Text(""));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
           
        }
    }
    
    public static void splitter(String fname){
    	String filePath = new File("").getAbsolutePath();
    	Integer count=0;
        try {
            Scanner s = new Scanner(new File(fname));
            while (s.hasNext()) {
            	count++;
            	s.next();
            }
            Integer cnt5=count/5;
            System.out.println(count);
            System.out.println(cnt5);
            Scanner sc = new Scanner(new File(fname));
            File file1 = new File("/home/hduser/data1.txt");
            File file2 = new File("/home/hduser/data2.txt");
            File file3 = new File("/home/hduser/data3.txt");
            File file4 = new File("/home/hduser/data4.txt");
            File file5 = new File("/home/hduser/data5.txt");
            file1.createNewFile();
            file2.createNewFile();
            file3.createNewFile();
            file4.createNewFile();
            file5.createNewFile();
            FileWriter fw1 = new FileWriter(file1.getAbsoluteFile());
			BufferedWriter bw1 = new BufferedWriter(fw1);
			FileWriter fw2 = new FileWriter(file2.getAbsoluteFile());
			BufferedWriter bw2 = new BufferedWriter(fw2);
			FileWriter fw3 = new FileWriter(file3.getAbsoluteFile());
			BufferedWriter bw3 = new BufferedWriter(fw3);
			FileWriter fw4 = new FileWriter(file4.getAbsoluteFile());
			BufferedWriter bw4 = new BufferedWriter(fw4);
			FileWriter fw5 = new FileWriter(file5.getAbsoluteFile());
			BufferedWriter bw5 = new BufferedWriter(fw5);
            for(int i=0;i<cnt5;i++){
            	String l=sc.next();
    			bw1.write(l+"\n");
            }
            for(int i=cnt5;i<2*cnt5;i++){
            	bw2.write(sc.next()+"\n");
    			
            }
            for(int i=2*cnt5;i<3*cnt5;i++){
            	bw3.write(sc.next()+"\n");
    			
            }
            for(int i=3*cnt5;i<4*cnt5;i++){
            	bw4.write(sc.next()+"\n");
    			
            }
            for(int i=4*cnt5;i<count;i++){
            	bw5.write(sc.next()+"\n");
    			
            }
            bw1.close();bw2.close();
            bw3.close();
            bw4.close();
            bw5.close();
            sc.close();
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    public static void test(String fname){
    	try {
            Scanner s = new Scanner(new File(fname));
            Double probs[]=new Double[probhmap.size()];
            int totallines=0;
            int positive=0;
            //System.out.println("class:"+probhmap.size());
            while (s.hasNext()) {
            	totallines++;
            	String line=s.nextLine();
            	String split[]=line.split(",");
            	Set<String> classlist=probhmap.keySet();
            	Iterator classitr=classlist.iterator();
            	int classi=0;
            	double maxprob=-1.0;
            	String classassg="";
            	while(classitr.hasNext()){
            		String hi=(String)classitr.next();
            		//System.out.println(hi);
            		probs[classi]=((double)probhmap.get(hi)/(double)total);
            		for(int i=0;i<split.length-1;i++){
            			int ll=0,ll1=0;
            			if(probhmap.containsKey(hi)){
            				ll=probhmap.get(hi);
            			}
            			if(probxhmap.containsKey(split[i]+"|"+hi)){
            				ll1=probxhmap.get(split[i]+"|"+hi);
            			}
            			
            			//System.out.println(split[i]);
                		
                		if(ll1!=0){
                			probs[classi]*=((double)probxhmap.get(split[i]+"|"+hi)/(double)probhmap.get(hi));
                		}
                		else{
                			probs[classi]=0.0;
                		}
            		}
            		if(probs[classi]>maxprob){
            			maxprob=probs[classi];
            			classassg=hi;
            		}
            		classi++;
            	}
            	//System.out.println(line);
            	//System.out.println(split.length);
            	
            	if(classassg.equalsIgnoreCase(split[10])){
            		positive++;
            	}
            	//s.next();
            }
            accuracy.add((double)positive/(double)totallines);
            //Integer cnt5=count/5;
            //Scanner sc = new Scanner(new File("/" + fname));
                        
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
    	splitter("/home/hduser/hw4data.csv");
    	
        //FileSystem hdfs=FileSystem.get(new Configuration());
        
        //System.out.println("1-----"+hdfs.getHomeDirectory());
        //Path inpdir=new Path(hdfs.getHomeDirectory().toString()+"/input");
        for(int i=1;i<=5;i++){
        	//hdfs.delete(inpdir,true);
        	//FileUtils.cleanDirectory(new File());
        	//hdfs.mkdirs(inpdir);
        	//FileUtils.cleanDirectory(new File("/output"));
//        	for(int j=1;j<=5;j++){
//        		if(j!=i){
//        			File source = new File("/home/hduser/data"+j+".txt");
//                	File dest = new File("/input");
//                	try {
//                		
//                		hdfs.copyFromLocalFile(new Path("/home/hduser/data"+j+".txt"),inpdir);
//                		//FileUtils.copyFileToDirectory(source, dest);
//                	    //FileUtils.copyDirectory(source, dest);
//                	} catch (IOException e) {
//                	    e.printStackTrace();
//                	}
//        		}
//        	}
        	Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "kmeans");
            job.setJarByClass(NBAYES_MR.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumCombiner.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, new Path("/input"+String.valueOf(i)));
            FileOutputFormat.setOutputPath(job, new Path("/output"));
            job.waitForCompletion(true);
            
            FileSystem fOpen = FileSystem.get(conf);
            Path outputPathReduceFile = new Path("/output/part-r-00000");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fOpen.open(outputPathReduceFile)));
            String Line=reader.readLine();
            //System.out.println(Line);
            while(Line!=null){
                String[] split=Line.split("_");
                String belongs[]=split[0].split(":");
                //System.out.println(Line);
                if(belongs[0].equalsIgnoreCase("X")){
                	probxmap.put(belongs[1], Integer.parseInt(split[1].trim()));
                }
                else if(belongs[0].equalsIgnoreCase("H")){
                	probhmap.put(belongs[1], Integer.parseInt(split[1].trim()));
                }
                else if(belongs[0].equalsIgnoreCase("X|H")){
                	//System.out.println(belongs[1]);
                	probxhmap.put(belongs[1], Integer.parseInt(split[1].trim()));
                }
                else{
                	total=Integer.parseInt(split[1].trim());
                }
                //probmap.put(split[0], Integer.parseInt(split[1]));
                Line=reader.readLine();
            }
            deleteFolder(conf,"/output");	
            test("/home/hduser/data"+i+".txt");
        	
        	
        }
        double avg=0.0;
        for(int i=0;i<accuracy.size();i++){
        	avg+=accuracy.get(i);
        }
        System.out.println("Accuracy : "+avg*100/5);
        
        
    }
    private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
    }

}