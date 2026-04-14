/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

/**
 * A variant of {@link TimestampIncrementingTableQuerier} that supports timestamp columns
 * stored as BIGINT (epoch milliseconds) instead of SQL TIMESTAMP types.
 */
public class TimestampBigIntIncrementingTableQuerier extends TimestampIncrementingTableQuerier {
  private static final Logger log = LoggerFactory.getLogger(
      TimestampBigIntIncrementingTableQuerier.class
  );

  public TimestampBigIntIncrementingTableQuerier(DatabaseDialect dialect, QueryMode mode,
                                                  String name,
                                                  String topicPrefix,
                                                  String bigIntTimestampColumnName,
                                                  String incrementingColumnName,
                                                  Map<String, Object> offsetMap,
                                                  Long timestampDelay,
                                                  TimeZone timeZone, String suffix,
                                                  TimestampGranularity timestampGranularity) {
    super(dialect, mode, name, topicPrefix,
        Collections.singletonList(bigIntTimestampColumnName),
        incrementingColumnName, offsetMap, timestampDelay,
        timeZone, suffix, timestampGranularity);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    log.debug("Creating PreparedStatement for BigInt timestamp mode");
    findDefaultAutoIncrementingColumn(db);

    ColumnId incrementingColumn = null;
    if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
      incrementingColumn = new ColumnId(tableId, incrementingColumnName);
    }

    ExpressionBuilder builder = dialect.expressionBuilder();
    switch (mode) {
      case TABLE:
        builder.append("SELECT * FROM ");
        builder.append(tableId);
        break;
      case QUERY:
        builder.append(query);
        break;
      default:
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
    }

    criteria = dialect.criteriaFor(incrementingColumn, timestampColumns, true);
    criteria.whereClause(builder);

    addSuffixIfPresent(builder);

    String queryString = builder.toString();
    recordQuery(queryString);
    log.trace("{} prepared SQL query: {}", this, queryString);
    stmt = dialect.createPreparedStatement(db, queryString);
  }
}
