package InvertedIndex;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class util {

    /**
     * ��ȡ��ǰ�������ļ���
     * @param context
     * @return
     */
    public  static  String setFilenameToK2(Mapper.Context context){
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        //�ɹ���ȡ�ļ������ļ������ȴ���0
        if (filename != null && !"".equals(filename)){
            return filename;
        }
        return "unkownfile.err";
    }

}
