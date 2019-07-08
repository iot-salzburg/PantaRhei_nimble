package com.github.christophschranz.adapter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamHubDTZ {

    public static void main(String[] args) {
        // write constants
        final String INPUT_TOPIC = "at.srfg.iot.dtz.data";
        final String [] OUTPUT_TOPICS = {"at.srfg.iot.nimbledc.external"};

        final String BOOTSTRAP_SERVERS_CONFIG = "192.168.48.71:9092,192.168.48.72:9092,192.168.48.73:9092,192.168.48.74:9092,192.168.48.75:9092";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streamhub-" + INPUT_TOPIC);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic, application logic
        KStream<String, String> inputTopic = streamsBuilder.stream(INPUT_TOPIC);
        KStream<String, String> filteredStream = inputTopic.filter((k, value) -> true);

        for (String topic: OUTPUT_TOPICS)
            if (topic.contains("external"))
                filteredStream.to(topic);
            else
                System.out.println("Check the OUTPUT_TOPICS: " + topic);

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties);

        // start our streams application
        kafkaStreams.start();
    }

    public static JsonParser jsonParser = new JsonParser();

    public static int extractUserFollowersInTweet(String tweetJson) {
        // json library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
