# PantaRhei_nimble

Integrate PantaRhei data to a database as it could have been negotiated through the nimble platform.

### Params of the PantaRhei Stack:

```java
    private static final String BOOTSTRAPSERVERS = "192.168.48.71:9092,192.168.48.72:9092,192.168.48.73:9092,192.168.48.74:9092,192.168.48.75:9092";
    private static final String GROUPID = "nimble-forwarder";
    private static final String[] TOPICS = {"at.srfg.iot.nimbledc.data", "at.srfg.iot.nimbledc.external"};
```   


### Params of the DB:

```
    "jdbc:postgresql://localhost:5432/nimbledc"
```   


### Params of the StreamHub Application:

```java
        final String INPUT_TOPIC = "at.srfg.iot.dtz.data";
        final String [] OUTPUT_TOPICS = {"at.srfg.iot.nimbledc.external"};
        final String BOOTSTRAP_SERVERS_CONFIG = "192.168.48.71:9092,192.168.48.72:9092,192.168.48.73:9092,192.168.48.74:9092,192.168.48.75:9092";
```

### Useful commands to extract data from the DB:

```sql
sudo -u postgres psql -d nimbledc -c "SELECT count(*) FROM data;"
sudo -u postgres psql -d nimbledc -c "SELECT * FROM data WHERE timestamp >= CAST(NOW()- interval '1 hour' AS text) AND id=69 ORDER BY timestamp DESC;"
```
