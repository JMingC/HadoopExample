package InvertedIndex;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class util {

    /**
     * 获取当前行所属文件名
     * @param context
     * @return
     */
    public  static  String setFilenameToK2(Mapper.Context context){
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        //成功获取文件名且文件名长度大于0
        if (filename != null && !"".equals(filename)){
            return filename;
        }
        return "unkownfile.err";
    }

}
