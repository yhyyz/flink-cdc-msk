### kda flink cdc to kafka

* KDA Flink(VERSION=1.15)
* Flink CDC DataStream API解析MySQL Binlog发送到Kafka  
* 自定义FlinkKafkaPartitioner, 数据库名，表名，主键值三个拼接作为partition key, 保证相同主键的记录发送到Kafka的同一个分区，保证消息顺序。
* Flink CDC支持增量快照算法，全局无锁，Batch阶段的checkpoint, 但需要表有主键，如果没有主键列增量快照算法就不可用，无法同步数据，需要设置scan.incremental.snapshot.enabled=false禁用增量快照

#### 使用方式
```shell
# Main Class : CDC2MSK
# 本地调试参数
-project_env local or prod # local表示本地运行，prod表示KDA运行
-disable_chaining false or true # 是否禁用flink operator chaining 
-delivery_guarantee at_least_once # kafka的投递语义at_least_once or exactly_once,建议at_least_once
-host localhost:3306 # mysql 地址
-username xxx # mysql 用户名
-password xxx # mysql 密码
-db_list test_db # 需要同步的数据库，支持正则，多个可以逗号分隔
-tb_list test_db.product.*,test_db.product  # 需要同步的表 支持正则，多个可以逗号分隔
-server_id 10000-10010 # 在快照读取之前，Source 不需要数据库锁权限。如果希望 Source 并行运行，则每个并行 Readers 都应该具有唯一的 Server id，所以 Server id 必须是类似 `5400-6400` 的范围，并且该范围必须大于并行度。
-server_time_zone Etc/GMT # mysql 时区
-position latest or initial # latest从当前CDC开始同步，initial先快照再CDC
-kafka_broker localhost:9092 # kafka 地址
-topic test-cdc-1 # topic 名称, 如果所有的数据都发送到同一个topic,设定要发送的topic名称
-topic_prefix flink_cdc_ # 如果按照数据库划分topic,不同的数据库中表发送到不同topic,可以设定topic前缀，topic名称会被设定为 前缀+数据库名。 设定了-topic_prefix参数后，-topic参数不再生效
-table_pk [{"db":"test_db","table":"product","primary_key":"pid"},{"db":"test_db","table":"product_01","primary_key":"pid"}] # 需要同步的表的主键

# KDA Console参数与之相同，去掉参数前的-即可 
# KDA种的参数组ID为: FlinkAppProperties
```

#### build
```sh
mvn clean package -Dscope.type=provided
```
   
