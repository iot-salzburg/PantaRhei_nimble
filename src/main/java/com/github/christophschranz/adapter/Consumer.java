package com.github.christophschranz.adapter;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;


public class Consumer {
    private String bootstrapserver;
    private String groupid;
    private String[] topics;
    private KafkaConsumer<String, String> consumer;

    private static HashMap<Integer,String> ds_mapping = new HashMap();
    private static DBProducer p = null;


    public Consumer(String bootstrapserver, String groupid, String[] topics) {
        this.bootstrapserver = bootstrapserver;
        this.groupid = groupid;
        this.topics = topics;
        // create Kafka Consumer
        consumer = createConsumer();

        // create DB Connection
        this.p = new DBProducer("jdbc:postgresql://localhost:5432/nimbledc",
                "postgres", "postgres");
        this.p.connectToDatabase();
        this.p.createDatabase();

        // ds_mapping
        ds_mapping.put(61, "at.srfg.iot.dtz.panda.End Effector vertical Force;N");
        ds_mapping.put(62, "at.srfg.iot.dtz.panda.End Effector x-Position;m");
        ds_mapping.put(63, "at.srfg.iot.dtz.panda.End Effector y-Position;m");
        ds_mapping.put(64, "at.srfg.iot.dtz.panda.End Effector z-Position;m");
        ds_mapping.put(65, "at.srfg.iot.dtz.panda.Actual Gripper Position;m");
        ds_mapping.put(68, "at.srfg.iot.dtz.panda.Robot X-Vibration;m/s");
        ds_mapping.put(69, "at.srfg.iot.dtz.panda.Robot Y-Vibration;m/s");

        for(HashMap.Entry<Integer,String> entry: ds_mapping.entrySet()) {
            HashMap<String,Object> metadata = new HashMap();
            metadata.put("id", entry.getKey());
            metadata.put("quantity", "'" + entry.getValue().substring(0, entry.getValue().indexOf(";")) + "'");
            metadata.put("unit",  "'" + entry.getValue().substring(entry.getValue().indexOf(";")+1) + "'");

            this.p.insertToDatabase("metadata", metadata);
        }
        System.out.println("Created Kafka Consumer");
    }


    public KafkaConsumer<String, String> createConsumer(){
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(this.topics));

        return consumer;
    }

    private static JsonParser jsonParser = new JsonParser();
    private static JsonObject extractIdFromData(String dataJson){
        // gson library
        return jsonParser.parse(dataJson)
                .getAsJsonObject();
    }
    public void start_forwarding() {
        boolean stopped = false;


        while (!stopped) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            if (recordCount > 0)
                System.out.println("Received " + recordCount + " records");

            for (ConsumerRecord<String, String> record : records) {

                try {
                    JsonObject payload = extractIdFromData(record.value());
                    int id = payload.getAsJsonObject("Datastream")
                            .getAsJsonPrimitive("@iot.id").getAsInt();
//                    System.out.println("Record with id " + id);
                    if (ds_mapping.containsKey(id)) {
                        HashMap<String,Object> datapoint = new HashMap();
                        datapoint.put("id", id);
                        datapoint.put("timestamp",
                                "'"+payload.getAsJsonPrimitive("phenomenonTime").getAsString()+"'");
                        datapoint.put("value", payload.getAsJsonPrimitive("result").getAsDouble());

//                        System.out.println("Record with id " + id + ": " + extractIdFromData(record.value()));

                        this.p.insertToDatabase("streamdata", datapoint);
                    }
                    Thread.sleep(0);
                } catch (NullPointerException e) {
                    System.out.println("skipping bad data: " + record.value());
                    stopped = true;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    stopped = true;
                }
            }
            //1102554178969354243, 1102554211328262144
            if ((recordCount > 0) & !stopped) {
//                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//                System.out.println("Committing offsets...");
                consumer.commitSync();
                System.out.println("Offsets have been committed");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close the client gracefully
        consumer.close();
    }
}
