package per.wanghai;

import java.io.*;

/**
 * @author 王海[https://github.com/AtTops]
 * @version V1.0
 * @package per.wanghai
 * @Description
 * @Date 2017/10/21 10:12
 */
// 缺省class:package-private
class WriteToFile {

    /**
     * @param str
     * @return file
     */
    static File createFile(String str){
        File file=new File("D:/JavaStudy/myudf/src/main",str);
        if(file.exists()){
            file.delete();
            System.out.println("文件已刪除");
            try {
                file.createNewFile();
            } catch (IOException e) {
                System.out.println(e+"删除后再创建出错");
            }
            System.out.println("删除后文件已创建");
        }
        else{
            try{
                file.createNewFile();
                System.out.println("文件已创建");
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        //FileOutputStream out=new FileOutputStream(“d:/myjava/write.txt ");
    return file;
    }

    /**
     * @param file
     * @return BufferedWriter
     */
    static BufferedWriter openBufferedWriter(File file){
        // 用BufferedWriter来写,它写入的是字符,所以不会仅写入一个字节,不会出现乱码（在处理字符流时涉及了字符编码的转换问题）.
        OutputStreamWriter outputStream = null;
        try {
            outputStream = new OutputStreamWriter(new FileOutputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        // 自定义缓冲区大小4096B
        BufferedWriter buffWriter = new BufferedWriter(outputStream,4096);
        return buffWriter;
    }

}
