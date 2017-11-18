package per.wanghai;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author 王海[https://github.com/AtTops]
 * @version V1.0
 * @package per.wanghai
 * @Description
 * @Date:2017/10/20 9:28
 */
public class RemoveQuotesUdf extends UDF{
    public Text evaluate(Text str){
        if(null == str){
            return null;
        }
        else if(null == str.toString()){
            return null;
        }
            else {
                return new Text(str.toString().replaceAll("\"",""));
            }
}

    public static void main(String[] args) {
        System.out.println(new RemoveQuotesUdf().evaluate(new Text("\"test_,\"666\"")));
    }
}
