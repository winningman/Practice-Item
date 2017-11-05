import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 王海
 * @version V1.0
 * @package per.wanghai
 * @Description
 * @Date 2017/10/29 23:23
 */
public class SomeHbaseAPI {
    private final Logger logger = LoggerFactory.getLogger(SomeHbaseAPI.class);

    protected void listTable(Admin admin) throws IOException {
        // 获得HTableDescriptors
        // （所有namespace的表，相当于scan META）
        HTableDescriptor[] tableDescriptor = admin.listTables();
        System.out.println("您的HBase有以下表:");
        for (int i = 0; i < tableDescriptor.length; i++) {
            System.out.println("表" + i + ":" + tableDescriptor[i].getNameAsString());
        }
    }

    /**
     * @param columnFamilies（这是一个变长参数，“Varargs”机制只允许一个变长参数，且必须放在最后）详见参考2
     * @throws IOException
     * @Description 该方法创建一个table实例
     */
    protected void createTable(Admin admin, TableName tableName, String... columnFamilies) throws IOException {
        try {
            if (admin.tableExists(tableName)) {
                // "{}"是slf4j的占位符（其一大特色）
                // DEBUG < INFO < WARN < ERROR < FATAL
                logger.warn("表:{}已经存在!", tableName.getNameAsString());
            } else {
                // 标注2：关于HTableDescriptor：
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for (String columnFamily : columnFamilies) {
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(tableDescriptor);
                logger.info("表:{}创建成功!", tableName.getNameAsString());
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    /**
     * @throws IOException
     * @Description 一行一行的插入数据
     * TODO：批量插入可以使用 Table.put(List<Put> list)
     */
    protected void putOneByOne(Connection connection, TableName tableName,
                               byte[] rowKey, String columnFamily, String column, String data) throws IOException {
        Table table = null;
        try {
            // 创建一个table实例
            table = connection.getTable(tableName);
            // HBase中所有的数据最终都被转化为byte[]
            // (rowKey已经在testCurd方法中转换为byte[])
            Put p = new Put(rowKey);
            // 查看源码知：put的add方法已经被弃用
            p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(p);
            logger.info("表:{}已更新！！！", tableName.getNameAsString());
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param connection
     * @param tableName
     * @param str:一个字符串数组（rowkey,family,qualifier,value.循环）
     * @throws IOException
     */
    protected void putList(Connection connection, TableName tableName, String[] str) throws IOException {
        // 每个put操作，我们放入四个数据
        int count = str.length / 4;
        // 我们希望数据量是4的倍数，因为剩下的我们将不会写入
        int remainder = str.length % 4;
        if (remainder > 0) {
            logger.warn("数据可能并不会像您预期的那样完全写入，如有必要，请检查下您的数据量！");
        }
        Table table = null;
        try {
            // 创建一个table实例
            table = connection.getTable(tableName);
            List<Put> puts = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Put put = new Put(Bytes.toBytes(str[4 * i]));
                put.addColumn(Bytes.toBytes(str[4 * i + 1]), Bytes.toBytes(str[4 * i + 2]), Bytes.toBytes(str[4 * i + 3]));
                puts.add(put);
            }
            table.put(puts);
            logger.info("表:{}已使用putList方法更新！！！", tableName.getNameAsString());
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @throws IOException
     * @Description 扫描表
     * 想获取部分行的数据，与putList方法类似，用List<Get>即可
     */
    protected void scan(Connection connection, TableName tableName) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            /*
            行的数目很大时，同时在一次请求中发送大量数据，会占用大量的系统资源并消耗很长时间，
            所以ResultScanner类把扫描操作转换为类似的get操作，它将每一行数据封装成一个Result实例，
            并将所有的Result实例放入一个迭代器中
             */
            ResultScanner rsScan1;
            ResultScanner rsScan2;

            // 这次操作返回表中所有的数据
            Scan scan1 = new Scan();
            rsScan1 = table.getScanner(scan1);
            for (Result r : rsScan1) {
                System.out.println(r);
                // 打印出来的Value是bytes类型
            }
            rsScan1.close();
            // 注：扫描器也使用同样的租约超时机制，保护其不被失效的客户单阻塞太久
            // 超时时间配置:hbase.regionserver.lease.period

            // 同样，也可以addfamily：
            Scan scan2 = new Scan();
            scan2.addFamily(Bytes.toBytes("commonInfo"));
            rsScan2 = table.getScanner(scan2);
            for (Result r : rsScan2) {
                System.out.println(r);
            }
            rsScan2.close();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @throws IOException
     * @Description 根据row key获取表中的该行数据
     */
    protected void getOneRow(Connection connection, TableName tableName, byte[] rowKey) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            // 这种方法获取指定rowkey的所有信息(然后可以使用不同的方法获取指定信息)
            // 用rowKey来实例化get对象，
            Get all = new Get(rowKey);
            // Result类不是线程安全的
            // 更多的使用方法见标注4
            Result result = table.get(all);

            // 可以使用addColumn指定columnFamily与qualifier
            // 标注3：更多缩小获取范围的方法

            /* 这里使用addFamily获取指定列族的所有列的信息（一行）
            Get part = new Get(rowKey);
            part.addFamily(Bytes.toBytes("commonInfo"));
            Result result = table.get(part);
            ...
            ...
            */

            /*通过getValue获取指定信息
            不推荐用字符串拼接的方式，字符串拼接会不断的创建新的对象，
            而原来的对象就会变为垃圾被GC回收掉，如果拼接得次数多，这样执行效率会很低底。
            （见下方Cell中使用StringBuffer）
            String city = Bytes.toString(result.getValue(Bytes.toBytes("commonInfo"),Bytes.toBytes("city")));
            String age = Bytes.toString(result.getValue(Bytes.toBytes("concelInfo"),Bytes.toBytes("age")));
            System.out.println("city: " + city + "\t" + "age: " + age);
            */

            // rawCells()返回cell[];注意：Cell接口中的getFamily、getValue等方法已经被废弃

            // 推荐：使用CellUtil中的一些列方法
            for (Cell cell : result.rawCells()) {
                /* 与上方的String拼接不同，这样的String拼接不会创建多个String对象
                System.out.println(
                "RowNum : " + "\t" + Bytes.toString(CellUtil.cloneRow(cell))
               + ", Family : " + "\t" + Bytes.toString(CellUtil.cloneFamily(cell))
               + ", Qualifier : " + "\t" + Bytes.toString(CellUtil.cloneQualifier(cell))
               + ", Value : " + "\t" + Bytes.toString(CellUtil.cloneValue(cell))
                );
                */

                // 采用StringBuffer：（因为其是可变的字符串对象，所以不会再创建新变量）
                StringBuffer sbuffer = new StringBuffer()
                        .append("RowNum : \t")
                        .append(Bytes.toString(CellUtil.cloneRow(cell)))
                        .append(", Family : \t")
                        .append(Bytes.toString(CellUtil.cloneFamily(cell)))
                        .append(", Qualifier : \t")
                        .append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                        .append(", Value : \t")
                        .append(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(sbuffer);
            }
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @throws IOException
     * @Description 删除表中的数据
     */
    protected void myDeleteTable(Admin admin, TableName tableName) throws IOException {
        try {
            if (admin.tableExists(tableName)) {
                // 必须先disable, 再delete
                myDisableTable(admin, tableName);
                // admin的很多方法在子类HBaseAdmin中实现
                // TODO：没看出该父类通过何种方式调用的子类方法
                admin.deleteTable(tableName);
                logger.info("表:{}已删除！！！", tableName.getNameAsString());
            } else {
                logger.info("表:{}并不存在！！！", tableName.getNameAsString());
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    protected void myDisableTable(Admin admin, TableName tableName) throws IOException {
        try {
            // admin的很多方法在子类HBaseAdmin中实现
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                logger.info("表:{}已禁用！！！", tableName.getNameAsString());
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }
}
