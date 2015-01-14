package com.github.t3t5u.common.ormlite;

import java.sql.SQLException;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.QueryBuilder;

public interface ThrowableQueryBuilder<E extends Entity> {
	QueryBuilder<E, Long> buildQuery(QueryBuilder<E, Long> queryBuilder) throws SQLException;
}
