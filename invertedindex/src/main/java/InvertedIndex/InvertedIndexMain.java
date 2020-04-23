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
        //���������쳣�򱨴�
        if (args.length != 2){
            System.err.println("ʹ�÷����� InvertedIndexMain <inĿ¼/�ļ�> <outĿ¼>");
            System.exit(2);
        }

        //ɾ��ԭ�������Ŀ¼
        final FileSystem fileSystem = FileSystem.get(new URI(args[0]),conf);
        if (fileSystem.exists(new Path(args[1]))){
            fileSystem.delete(new Path(args[1]),true);
        }
        //�м�����Ŀ¼
        if (fileSystem.exists(new Path(TEMP_PATH))){
            fileSystem.delete(new Path(TEMP_PATH),true);
        }

        //����һ�����Ƽ������������ս������  ����-���ļ��� ���ʳ��ִ���
        Job job_one=Job.getInstance(conf, "JobOne");
        //���������Ӧ���࣬�����Ӧ��Mapper��Reducer��Mapper��Reduer��Ӧ�����������
        job_one.setJarByClass(InvertedIndexMain.class);
        job_one.setMapperClass(JobOneMapper.class);
        job_one.setReducerClass(JobOneReducer.class);
        job_one.setMapOutputKeyClass(Text.class);
        job_one.setMapOutputValueClass(IntWritable.class);
        job_one.setOutputKeyClass(Text.class);
        job_one.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job_one,new Path(args[0]));
        FileOutputFormat.setOutputPath(job_one,new Path(TEMP_PATH));


        //����������ս������  ����-�����ļ��������ʳ��ִ��� �ļ��������ʳ��ִ��� �ļ��������ʳ��ִ�����
        Job job_two=Job.getInstance(conf, "JobTwo");
        //���������Ӧ���࣬�����Ӧ��Mapper��Reducer��Mapper��Reduer��Ӧ�����������
        job_two.setJarByClass(InvertedIndexMain.class);
        //����K1,V1ΪText��Text����
        job_two.setInputFormatClass(KeyValueTextInputFormat.class);
        job_two.setMapperClass(JobTwoMapper.class);
        job_two.setReducerClass(JobTwoReducer.class);
        job_two.setMapOutputKeyClass(Text.class);
        job_two.setMapOutputValueClass(Text.class);
        job_two.setOutputKeyClass(Text.class);
        job_two.setOutputValueClass(Text.class);
        //���÷���,�ֱ���A-M��ͷ,N-Z��ͷ,���࿪ͷ����
        job_two.setPartitionerClass(InvIndPartitioner.class);
        job_two.setNumReduceTasks(3);
        FileInputFormat.addInputPath(job_two,new Path(TEMP_PATH));
        FileOutputFormat.setOutputPath(job_two,new Path(args[1]));

        //�ύ����one��one��ɺ��ִ������two
        if (job_one.waitForCompletion(true)){
            System.exit(job_two.waitForCompletion(true) ? 0 : 1);
        }
    }

}
