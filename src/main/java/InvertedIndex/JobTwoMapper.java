package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * JobTwo��Mapper�׶�תΪ�����ʣ����ļ�����N����
 */
public class JobTwoMapper extends Mapper<Text, Text,Text,Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = key.toString().split(":");
        //����
        keyText.set(row[0]);
        //value
        valueText.set( row[1] + "," + value.toString());
        context.write(keyText,valueText);
    }
}
