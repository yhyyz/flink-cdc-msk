package com.aws.analytics.tools;


import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class DebeziumConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private static final Logger log = LoggerFactory.getLogger(DebeziumConverter.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter timeFormatter;
    private DateTimeFormatter datetimeFormatter;
    private SchemaBuilder schemaBuilder;
    private String databaseType;
    private String schemaNamePrefix;
    // 获取默认时区
    private final ZoneId zoneId = ZoneOffset.systemDefault();

    @Override
    public void configure(Properties properties) {
        // 必填参数：database.type。获取数据库的类型，暂时支持mysql、sqlserver
        this.databaseType = properties.getProperty("database.type");
        // 如果未设置，或者设置的不是mysql、sqlserver，则抛出异常。
        if (this.databaseType == null || (!this.databaseType.equals("mysql") && !this.databaseType.equals("sqlserver"))) {
            throw new IllegalArgumentException("database.type 必须设置为 'mysql' or 'sqlserver'");
        }
        // 选填参数：format.date、format.time、format.datetime。获取时间格式化的格式
        String dateFormat = properties.getProperty("format.date", DATE_FORMAT);
        String timeFormat = properties.getProperty("format.time", TIME_FORMAT);
        String datetimeFormat = properties.getProperty("format.datetime", DATETIME_FORMAT);
        // 获取自身类的包名+数据库类型为默认schema.name
        String className = this.getClass().getName();
        // 查看是否设置schema.name.prefix
        this.schemaNamePrefix = properties.getProperty("schema.name.prefix", className + "." + this.databaseType);
        // 初始化时间格式化器
        dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
        datetimeFormatter = DateTimeFormatter.ofPattern(datetimeFormat);
    }

    // mysql的转换器
    public void registerMysqlConverter(String columnType, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String schemaName = this.schemaNamePrefix + "." + columnType.toLowerCase();
        schemaBuilder = SchemaBuilder.string().name(schemaName);
        switch (columnType) {
            case "DATE":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.time.LocalDate) {
                        return dateFormatter.format((java.time.LocalDate) value);
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            case "TIME":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.time.Duration) {
                        return timeFormatter.format(
                                java.time.LocalTime.
                                        ofNanoOfDay(((java.time.Duration) value)
                                                .toNanos()));
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            case "DATETIME":
            case "TIMESTAMP":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.time.LocalDateTime) {
                        return datetimeFormatter.format((java.time.LocalDateTime) value);
                    } else if (value instanceof java.time.ZonedDateTime) {
                        return datetimeFormatter.format(((java.time.ZonedDateTime) value).withZoneSameInstant(zoneId).toLocalDateTime());
                    } else if (value instanceof java.sql.Timestamp) {
                        if (columnType.equalsIgnoreCase("datetime")){
                            return datetimeFormatter.format(((java.sql.Timestamp) value).toLocalDateTime().atZone(ZoneId.of("UTC")).withZoneSameInstant(ZoneId.of("GMT+08:00")));
                        }
                        return datetimeFormatter.format(((java.sql.Timestamp) value).toLocalDateTime().atZone(ZoneId.of("GMT+08:00")).withZoneSameInstant(ZoneId.of("GMT+08:00")));
                    } else if (value instanceof java.lang.String) {
                        Instant instant = Instant.parse((String) value);
                        java.time.LocalDateTime dateTime = java.time.LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                        return datetimeFormatter.format(dateTime);
                    }
                    else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            default:
                schemaBuilder = null;
                break;
        }
    }

    @Override
    public void converterFor(RelationalColumn relationalColumn, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String columnType = relationalColumn.typeName().toUpperCase();
        log.info("database:{},columns name：{}，type：{},jdbc type :{}", this.databaseType, relationalColumn.name(),columnType, relationalColumn.jdbcType());
        if (this.databaseType.equals("mysql")) {
            this.registerMysqlConverter(columnType, converterRegistration);
        } else {
            log.warn("===failed unsupport database: {}", this.databaseType);
            schemaBuilder = null;
        }
    }

    private String getClassName(Object value) {
        if (value == null) {
            return null;
        }
        return value.getClass().getName();
    }

    private String failConvert(Object value, String type) {
        String valueClass = this.getClassName(value);
        String valueString = valueClass==null?null:value.toString();
        log.warn("===failed {} convert type，type：{}，value：{}", type, valueClass, valueString);
        return valueString;
    }
}