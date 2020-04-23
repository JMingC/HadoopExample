package InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 将 Mapper结果 《 单词，(文件名,出现次数) 》 转为 《单词，(文件名,出现次数 文件名,2 ···)》
 */
public class JobTwoReducer extends Reducer<Text, Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder finalValBulider = new StringBuilder("");
        for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();){
            finalValBulider.append(iterator.next().toString());
            //不是最后一个则加空格
            if (iterator.hasNext()){
                finalValBulider.append(" ");
            }
        }
        context.write(key,new Text(finalValBulider.toString()));
    }
}
