package InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvIndPartitioner extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text text, Text text2, int i) {
        String key = text.toString();
        char head = key.charAt(0);
        if ( (head>='A' && head<='M') || (head>='a' && head<='m') ){
            //A-M,a-m
            return 0;
        }else if ( (head>='N' && head<='Z') || (head>='n' && head<='z') ){
            //N-Z,n-z
            return 1;
        }else {
            //Other
            return 2;
        }
    }

}
