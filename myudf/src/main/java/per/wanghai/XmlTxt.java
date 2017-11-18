package per.wanghai;

/**
 * @author 王海[https://github.com/AtTops]
 * @version V1.0
 * @package per.wanghai
 * @Description
 * @Date 2017/10/20 20:50
 */

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class XmlTxt {
    private String TXT_FLIENAME = "weibo_3500014.txt";
    String XML_FLIENAME = "D:/JavaStudy/myudf/src/main/weibo_3500014.xml";
    // 对程序运行时间进行记录
    long startTime = System.currentTimeMillis();
    // 调用同一个包中的WriteToFile类的createFlies方法,返回file
    File file_return = WriteToFile.createFile(TXT_FLIENAME);

    // 调用WriteToFile类的BufferedWriter方法，返回buffWriter
    BufferedWriter buffWriter = WriteToFile.openBufferedWriter(file_return);

    /**
     * @throws Exception
     */
    private void test() throws Exception {
        // 创建SAXReader对象
        SAXReader reader = new SAXReader();
        // 读取文件,转换成Document
        Document document = reader.read(new File(XML_FLIENAME));
        // 获取根节点元素对象
        Element root = document.getRootElement();
        /* elementiterator()  方法获取是该节点的孩子节点。
         但某个孩子节点还有子节点，这些子节点并不在这个方法获取的节点中
         (也就是说这里我们获取所有的<person>节点)
        */
        Iterator<Element> iterator = root.elementIterator();
        while (iterator.hasNext()) {
            Element node = iterator.next();
            // 同时迭代当前节点下面的所有一级子节点（我们这里person节点的子节点再无子节点）
            List<Element> listElement = node.elements();
            for (Element e : listElement) {
                // 获取该节点下的信息
                getMassages(e);
            }
            // 一个person节点读取完毕，文件换行。然后读取下一个节点的信息
            buffWriter.newLine();
        }
        // 所有节点读取并写入完毕
        buffWriter.flush();
        buffWriter.close();
        long consumed_time = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("程序运行完毕！,共消耗" + consumed_time + "秒！！！");
    }

    private void getMassages(Element node) throws IOException {
        /* 测试用
        System.out.println("当前节点的名称：" + node.getName());
        */
        if (!(node.getTextTrim().equals(""))) {
            // 如果当前节点内容不为空，则输出;否则输出“null”
            // 为了避免在hive分割字符串时出错，我用 （`） 作分隔符
            String str1 = node.getText() + "`";
            buffWriter.write(str1);
        } else {
            String str2 = "null" + "`";
            buffWriter.write(str2);
        }
    }

    public static void main(String[] args) {
        XmlTxt xmlTxt;
        xmlTxt = new XmlTxt();
        try {
            xmlTxt.test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
