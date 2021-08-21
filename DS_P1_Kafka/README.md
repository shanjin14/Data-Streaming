### Kafka project


#### Project Overview
1. Build a set of producer to publish data to kafka
2. Build a faust stream processing to transform a data and sink it to kafka
3. Build a ksql script and sink it to kafka
4. Build a set of consumers to consumer the data

#### After thought
1. quite a fair bit of consideration when comes to streaming, especially 
2. kafka can help to achieve fault tolerance, however i think we need to handle other exception handling such as (duplicate publishing, duplicate consuming)
3. More documentation reading can be found here: [link](https://kafka.apache.org/)

#### Note
1. the code base is from Udacity. Quite a fair bit of hand-holding code in going through this.
