package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner����Ϊ������<k2,v2> = <"�ļ���a:����X","1"> �ϲ�Ϊ <"�ļ���a:����X","N">
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
        //�޸������ʽΪ ����a->�ļ���a��N
        String[] oldKey = key.toString().split(":");
        String newKey = oldKey[1];
        String filename = oldKey[0];
        context.write(new Text(newKey),new Text(filename + "," + sum));
    }
}
