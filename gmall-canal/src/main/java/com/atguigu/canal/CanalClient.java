package com.atguigu.canal;

import com.aatguigu.gmall.constant.GmallConstants;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.utils.KafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.lang.model.element.VariableElement;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author Skipper
 * @date 2020/08/18
 * @desc
 */
public class CanalClient {
    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "",
                "");

        while (true) {
            canalConnector.connect();

            canalConnector.subscribe("gmall200317.*");

            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //获取message中的集合并遍历
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //获取entry中的Rowdata类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            //获取entry中的表名和数据
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValue = entry.getStoreValue();
                            //反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            processData(tableName, eventType, rowDatasList);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

    }

    private static void processData(String tableName, CanalEntry.EventType eventType,
                                    List<CanalEntry.RowData> rowDatasList) {
        //需求:只要order_info中的数据

        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){

            //遍历集合
            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(),column.getValue());
                }

                System.out.println(jsonObject.toString());

                KafkaSender.sendToKafka(GmallConstants.GMALL_TOPIC_ORDER_INFO,jsonObject.toString());
            }


        }
    }
}
