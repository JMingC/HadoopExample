package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * JobOne的Map，输入数据为（行号，文本），输出数据为（单词：文件名，1）
 */
public class JobOneMapper extends Mapper<Object, Text,Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();
        StringTokenizer strt = new StringTokenizer(value.toString());
        while (strt.hasMoreTokens()){

            word.set(name+ ":" + strt.nextToken());
            context.write(word,one);
        }

    }


}
