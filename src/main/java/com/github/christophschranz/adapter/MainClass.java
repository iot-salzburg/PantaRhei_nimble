package com.github.christophschranz.adapter;

// CREATE TABLE IF NOT EXISTS Metadata (id integer NOT NULL PRIMARY KEY, quantity varchar(50) NOT NULL, unit varchar(15));
// CREATE TABLE IF NOT EXISTS Streamdata (id integer NOT NULL, timestamp datetime NOT NULL, value NOT NULL, PRIMARY KEY (id, timestamp));
// CREATE OR REPLACE VIEW data AS SELECT id, timestamp, quantity, value, unit FROM metadata NATURAL JOIN streamdata;

// SELECT data from the last hour:
// SELECT * FROM data WHERE timestamp >= CAST(NOW()- interval '1 day' AS text);
// sudo -u postgres psql -d nimbledc -c "SELECT * FROM data WHERE timestamp >= CAST(NOW()- interval '1 hour' AS text) AND id=61;"
public class MainClass {
    private static final String BOOTSTRAPSERVERS = "192.168.48.71:9092,192.168.48.72:9092,192.168.48.73:9092,192.168.48.74:9092,192.168.48.75:9092";
    private static final String GROUPID = "nimble-forwarder";
    private static final String[] TOPICS = {"at.srfg.iot.nimbledc.data", "at.srfg.iot.nimbledc.external"};


    public static void main(String[] args) {
        Consumer client = new Consumer(BOOTSTRAPSERVERS, GROUPID, TOPICS);
        System.out.println("Created Kafka Consumer");
        client.start_forwarding();
    }
}
