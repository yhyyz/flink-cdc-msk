### kda flink cdc to kafka

```markdown
* KDA Flink(VERSION=1.15)
* Flink CDC DataStream API解析MySQL Binlog发送到Kafka，支持按库发送到不同Topic, 也可以发送到同一个Topic
* 自定义FlinkKafkaPartitioner, 数据库名，表名，主键值三个拼接作为partition key, 保证相同主键的记录发送到Kafka的同一个分区，保证消息顺序。
* Flink CDC支持增量快照算法，全局无锁，Batch阶段的checkpoint, 但需要表有主键，如果没有主键列增量快照算法就不可用，无法同步数据，需要设置scan.incremental.snapshot.enabled=false禁用增量快照
* 当前加入了MySQL,Mongo
* 支持MySQL指定从binlog位置或者binlog时间点解析数据
* 加入EMR on EC2支持(flink 1.15.x version)
```

#### 更新
* 2024-07-02 EMR-6.11.0 flink-1.16.0 tpcc测试
* 2023-10-10 增加Debezium Custom Converter处理Datetime类型转换和时区问题
* 2023-09-08 增加对指定字段进行长度截取
```shell
加入-chunk_size参数,默认值8096,全量阶段如果表比较大,表的单行数据比较大,产生OOM时可以调小该值
```
* 2023-08-14 增加对指定字段进行长度截取
```shell
-table_pk 参数中指定column_max_length参数,col1=10|col2=10,表示col1列保留10个字符,col2列保留10个字符,多个列以竖线分割,注意-table_pk的json参数需要用反斜杠转义,例子如下
'[{\"db\":\"test_db\",\"table\":\"product\",\"primary_key\":\"id\",\"column_max_length\":\"col1=10|col2=10\"}]'
# jar下载：https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202310101435.jar
```

#### 使用方式
```shell
# Main Class : MySQLCDC2AWSMSK
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
-position latest or initial or mysql-bin.000003 or mysql-bin.000003:123 or gtid:24DA167-0C0C-11E8-8442-00059A3C7B00:1-19 or timestamp:1667232000000 # latest从当前CDC开始同步，initial先快照再CDC, binlog_file_name 指定binlog文件, binlog_file_name:position 指定binlog文件和位置,gtid:xxx 指定gtid, timestamp:13位时间戳 指定时间戳
-kafka_broker localhost:9092 # kafka 地址
-topic test-cdc-1 # topic 名称, 如果所有的数据都发送到同一个topic,设定要发送的topic名称
-topic_prefix flink_cdc_ # 如果按照数据库划分topic,不同的数据库中表发送到不同topic,可以设定topic前缀，topic名称会被设定为 前缀+数据库名。 设定了-topic_prefix参数后，-topic参数不再生效
-table_pk [{"db":"test_db","table":"product","primary_key":"pid"},{"db":"test_db","table":"product_01","primary_key":"pid"}] # 需要同步的表的主键
# max.request.size 默认1MB,这里设置的10MB
-kafka_properties 'max.request.size=1073741824,xxxx=xxxx' # kafka生产者参数,多个以逗号分隔
-chunk_size 8090 # 默认值8096，全量阶段如果表比较大，表的单行数据比较大，产生OOM时，可以调小该值

# KDA Console参数与之相同，去掉参数前的-即可 
# KDA种的参数组ID为: FlinkAppProperties
```

```shell
# Main Class : MongoCDC2AWSMSK
# 本地调试参数
-project_env local or prod # local表示本地运行，prod表示KDA运行
-disable_chaining false or true # 是否禁用flink operator chaining 
-delivery_guarantee at_least_once # kafka的投递语义at_least_once or exactly_once,建议at_least_once
-host localhost:27017 # mongo 地址
-username xxx # mongo 用户名
-password xxx # mongo 密码
-db_list test_db # 需要同步的数据库，支持正则，多个可以逗号分隔
-collection_list test_db.product.*,test_db.product  # 需要同步的表 支持正则，多个可以逗号分隔
-copy_existing true or flase # true表示先同步已有数据再change streams cdc
-kafka_broker localhost:9092 # kafka 地址
-topic test-cdc-1 # topic 名称, 如果所有的数据都发送到同一个topic,设定要发送的topic名称
-topic_prefix flink_cdc_ # 如果按照数据库划分topic,不同的数据库中表发送到不同topic,可以设定topic前缀，topic名称会被设定为 前缀+数据库名。 设定了-topic_prefix参数后，-topic参数不再生效
-kafka_properties 'max.request.size=1073741824,xxxx=xxxx' # kafka生产者参数,多个以逗号分隔

# KDA Console参数与之相同，去掉参数前的-即可 
# KDA种的参数组ID为: FlinkAppProperties
```

#### build
```sh
mvn clean package -Dscope.type=provided

# mysql cdc
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202310101435.jar

# mysql cdc flink -1.16.0
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202406271942.jar


# mongo cdc
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-mongo-cdc-msk-1.0-SNAPSHOT-202305242104.jar

```
   
#### EMR on EC2
##### EMR 6.11.0的flink 1.16版本
###### tpcc test
* msk topic
```shell
export bs="b-1.cdctestnew.xxxx.kafka.ap-northeast-1.amazonaws.com:9092"
cd /home/ec2-user/kafka
./bin/kafka-topics.sh --create --bootstrap-server $bs --replication-factor 2 --partitions 12 --topic flink_cdc_test
./bin/kafka-topics.sh --describe --bootstrap-server $bs --topic flink_cdc_test
./bin/kafka-console-consumer.sh --bootstrap-server  $bs  --topic cdc_test --from-beginning |more
./bin/kafka-consumer-groups.sh --bootstrap-server $bs --describe  --group cdc-redshift-ec2-g1
```
* flink job
```sh
# disable check-leaked-classloader
sudo sed -i -e '$a\classloader.check-leaked-classloader: false' /etc/flink/conf/flink-conf.yaml
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202406271942.jar

s3_bucket_name="flink-cdc-test"
sudo flink run  -m yarn-cluster  -yjm 4096 -ytm 6144 -d -ys 1  \
-c  com.aws.analytics.emr.MySQLCDC2AWSMSK \
/home/hadoop/flink-cdc-msk-1.0-SNAPSHOT-202406271942.jar \
-project_env prod \
-disable_chaining true \
-delivery_guarantee at_least_once \
-host flink-cdc-new-instance-1.xxxxx.ap-northeast-1.rds.amazonaws.com:3306 \
-username xxx \
-password xxx \
-db_list tpcc \
-tb_list tpcc.warehouse,tpcc.district,tpcc.customer,tpcc.new_orders,tpcc.orders,tpcc.order_line,tpcc.item,tpcc.stock \
-server_id 200200-200300 \
-server_time_zone Etc/GMT \
-position latest \
-kafka_broker b-1.xxxxxx.c2.kafka.ap-northeast-1.amazonaws.com:9092 \
-topic flink_cdc_test \
-table_pk '[{\"db\":\"tpcc\",\"table\":\"warehouse\",\"primary_key\":\"w_id\"},{\"db\":\"tpcc\",\"table\":\"district\",\"primary_key\":\"d_w_id,d_id\"},{\"db\":\"tpcc\",\"table\":\"customer\",\"primary_key\":\"c_w_id,c_d_id,c_id\"},{\"db\":\"tpcc\",\"table\":\"new_orders\",\"primary_key\":\"no_w_id,no_d_id,no_o_id\"},{\"db\":\"tpcc\",\"table\":\"orders\",\"primary_key\":\"o_w_id,o_d_id,o_id\"},{\"db\":\"tpcc\",\"table\":\"order_line\",\"primary_key\":\"ol_w_id,ol_d_id,ol_o_id,ol_number\"},{\"db\":\"tpcc\",\"table\":\"item\",\"primary_key\":\"i_id\"},{\"db\":\"tpcc\",\"table\":\"stock\",\"primary_key\":\"s_w_id,s_i_id\"}]' \
-checkpoint_interval 60 \
-checkpoint_dir s3://${s3_bucket_name}/flink/checkpoint/test/ \
-parallel 6 \
-kafka_properties 'max.request.size=1073741824' \
-chunk_size 4000

```

##### EMR 6.8.0+的flink 1.15版本
```sh
# s3 plugin
sudo mkdir -p /usr/lib/flink/plugins/s3/
sudo mv  /usr/lib/flink/opt/flink-s3-fs-hadoop-1.15.1.jar /usr/lib/flink/plugins/s3/
# disable check-leaked-classloader
sudo sed -i -e '$a\classloader.check-leaked-classloader: false' /etc/flink/conf/flink-conf.yaml
```

##### run job
```sh
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202310101435.jar

s3_bucket_name="panchao-data"
sudo flink run -s s3://${s3_bucket_name}/flink/checkpoint/test/eb2bebad3cc51afd83183a8b38a927a6/chk-3/  -m yarn-cluster  -yjm 1024 -ytm 2048 -d -ys 4  \
-c  com.aws.analytics.emr.MySQLCDC2AWSMSK \
/home/hadoop/flink-cdc-msk-1.0-SNAPSHOT-202310101435.jar \
-project_env prod \
-disable_chaining true \
-delivery_guarantee at_least_once \
-host ssa-panchao-db.cojrbrhcpw9s.us-east-1.rds.amazonaws.com:3306 \
-username ssa \
-password Ssa123456 \
-db_list test_db \
-tb_list test_db.product.* \
-server_id 200200-200300 \
-server_time_zone Etc/GMT \
-position timestamp:1688204585000 \
-kafka_broker b-1.commonmskpanchao.wp46nn.c9.kafka.us-east-1.amazonaws.com:9092 \
-topic test-cdc-1 \
-table_pk '[{\"db\":\"cdc_db_02\",\"table\":\"sbtest2\",\"primary_key\":\"id\"},{\"db\":\"test_db\",\"table\":\"product\",\"primary_key\":\"id\",\"column_max_length\":\"name=3\"}]'  \
-checkpoint_interval 30 \
-checkpoint_dir s3://${s3_bucket_name}/flink/checkpoint/test/ \
-parallel 4 \
-kafka_properties 'max.request.size=1073741824' 
-chunk_size 8090 # 默认值8096，全量阶段如果表比较大，表的单行数据比较大，产生OOM时，可以调小该值
# max.request.size 默认1MB,这里设置的10MB

# 如果从savepoint或者checkpoint恢复作业，flink run -s 参数指定savepoint或者最后一次checkpoint目录
# eg: s3://${s3_bucket_name}/flink/checkpoint/test/eb2bebad3cc51afd83183a8b38a927a6/chk-3/
# 手动触发savepoint
# eg: flink savepoint bfee56b39a69f654b4b444510581327b  s3://${s3_bucket_name}/flink/savepoint/ -yid application_1672837624250_0011
```

#### 如果使用EMR上的Flink版本低于1.15,比如EMR 6.7.0，可以使用如下方式，使用flink 1.15.4
```sh
# login emr master node  and download flink1.15.4
wget -P /home/hadoop/ https://dlcdn.apache.org/flink/flink-1.15.4/flink-1.15.4-bin-scala_2.12.tgz
# 备用链接 https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-1.15.4-bin-scala_2.12.tgz


# set table planner
cd /home/hadoop/ && tar -xvzf flink-1.15.4-bin-scala_2.12.tgz
mv /home/hadoop/flink-1.15.4/lib/flink-table-planner-loader-1.15.4.jar /home/hadoop/flink-1.15.4/opt/
cp /home/hadoop/flink-1.15.4/opt/flink-table-planner_2.12-1.15.4.jar /home/hadoop/flink-1.15.4/lib/
cp /home/hadoop/flink-1.15.4/opt/flink-s3-fs-hadoop-1.15.4.jar /home/hadoop/flink-1.15.4/lib/

# disable check-leaked-classloader
sed -i -e '$a\classloader.check-leaked-classloader: false' /home/hadoop/flink-1.15.4/conf/flink-conf.yaml
# 修改restart 策略，如果失败立即退出
sed -i -e '$a\restart-strategy: fixed-delay ' /home/hadoop/flink-1.15.4/conf/flink-conf.yaml

```
##### run job
```sh
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/flink-cdc-msk-1.0-SNAPSHOT-202310101435.jar


export HADOOP_CLASSPATH=`hadoop classpath`
s3_bucket_name="panchao-data"
/home/hadoop/flink-1.15.4/bin/flink run -m yarn-cluster  -yjm 1024 -ytm 2048 -d -ys 4  \
-D restart-strategy.type=fixed-delay -D restart-strategy.fixed-delay.attempts=1 -D restart-strategy.fixed-delay.delay=3 \
-c  com.aws.analytics.emr.MySQLCDC2AWSMSK \
/home/hadoop/flink-cdc-msk-1.0-SNAPSHOT-202310101435.jar \
-project_env prod \
-disable_chaining true \
-delivery_guarantee at_least_once \
-host ssa-panchao-db.cojrbrhcpw9s.us-east-1.rds.amazonaws.com:3306 \
-username ssa \
-password Ssa123456 \
-db_list test_db \
-tb_list test_db.product.* \
-server_id 200200-200300 \
-server_time_zone Etc/GMT \
-position timestamp:1690900632000 \
-kafka_broker b-1.commonmskpanchao.wp46nn.c9.kafka.us-east-1.amazonaws.com:9092 \
-topic test-cdc-1 \
-table_pk '[{\"db\":\"cdc_db_02\",\"table\":\"sbtest2\",\"primary_key\":\"id\"},{\"db\":\"test_db\",\"table\":\"product\",\"primary_key\":\"id\",\"column_max_length\":\"name=3\"}]'  \
-checkpoint_interval 30 \
-checkpoint_dir s3://${s3_bucket_name}/flink/checkpoint/test/ \
-parallel 4 \
-kafka_properties 'max.request.size=1073741824' \
-chunk_size 8090 # 默认值8096，全量阶段如果表比较大，表的单行数据比较大，产生OOM时，可以调小该值
# max.request.size 默认1MB,这里设置的10MB,多个参数逗号分隔

# 如果从savepoint或者checkpoint恢复作业，/home/hadoop/flink-1.15.4/bin/flink  run -s 参数指定savepoint或者最后一次checkpoint目录
# eg: s3://${s3_bucket_name}/flink/checkpoint/test/eb2bebad3cc51afd83183a8b38a927a6/chk-3/
# 手动触发savepoint
# eg: /home/hadoop/flink-1.15.4/bin/flink  savepoint bfee56b39a69f654b4b444510581327b  s3://${s3_bucket_name}/flink/savepoint/ -yid application_1672837624250_0011
```
