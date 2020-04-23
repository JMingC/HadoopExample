package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class InvertedIndexMain {

    private static final String TEMP_PATH = "/output/tmp";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf=new Configuration();
        //参数数量异常则报错
        if (args.length != 2){
            System.err.println("使用方法： InvertedIndexMain <in目录/文件> <out目录>");
            System.exit(2);
        }

        //删除原来的输出目录
        final FileSystem fileSystem = FileSystem.get(new URI(args[0]),conf);
        if (fileSystem.exists(new Path(args[1]))){
            fileSystem.delete(new Path(args[1]),true);
        }
        //中间数据目录
        if (fileSystem.exists(new Path(TEMP_PATH))){
            fileSystem.delete(new Path(TEMP_PATH),true);
        }

        //任务一，类似计数，但是最终结果形如  单词-》文件名 单词出现次数
        Job job_one=Job.getInstance(conf, "JobOne");
        //设置任务对应主类，任务对应的Mapper及Reducer，Mapper和Reduer对应输入输出类型
        job_one.setJarByClass(InvertedIndexMain.class);
        job_one.setMapperClass(JobOneMapper.class);
        job_one.setReducerClass(JobOneReducer.class);
        job_one.setMapOutputKeyClass(Text.class);
        job_one.setMapOutputValueClass(IntWritable.class);
        job_one.setOutputKeyClass(Text.class);
        job_one.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job_one,new Path(args[0]));
        FileOutputFormat.setOutputPath(job_one,new Path(TEMP_PATH));


        //任务二，最终结果形如  单词-》（文件名，单词出现次数 文件名，单词出现次数 文件名，单词出现次数）
        Job job_two=Job.getInstance(conf, "JobTwo");
        //设置任务对应主类，任务对应的Mapper及Reducer，Mapper和Reduer对应输入输出类型
        job_two.setJarByClass(InvertedIndexMain.class);
        //输入K1,V1为Text，Text类型
        job_two.setInputFormatClass(KeyValueTextInputFormat.class);
        job_two.setMapperClass(JobTwoMapper.class);
        job_two.setReducerClass(JobTwoReducer.class);
        job_two.setMapOutputKeyClass(Text.class);
        job_two.setMapOutputValueClass(Text.class);
        job_two.setOutputKeyClass(Text.class);
        job_two.setOutputValueClass(Text.class);
        //设置分区,分别处理A-M开头,N-Z开头,其余开头单词
        job_two.setPartitionerClass(InvIndPartitioner.class);
        job_two.setNumReduceTasks(3);
        FileInputFormat.addInputPath(job_two,new Path(TEMP_PATH));
        FileOutputFormat.setOutputPath(job_two,new Path(args[1]));

        //提交任务one，one完成后才执行任务two
        if (job_one.waitForCompletion(true)){
            System.exit(job_two.waitForCompletion(true) ? 0 : 1);
        }
    }

}
