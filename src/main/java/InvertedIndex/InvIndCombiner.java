package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner任务为将所有<k2,v2> = <"文件名a:单词X","1"> 合并为 <"文件名a:单词X","N">
 */
public class InvIndCombiner extends Reducer<Text, IntWritable,Text,Text> {
    private Text combineVal = new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        int sum = 0;
        for (IntWritable val : values){
            sum += val.get();
        }
        //修改输出格式为 单词a->文件名a，N
        String[] oldKey = key.toString().split(":");
        String newKey = oldKey[1];
        String filename = oldKey[0];
        context.write(new Text(newKey),new Text(filename + "," + sum));
    }
}
