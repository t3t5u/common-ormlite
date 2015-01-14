package com.github.t3t5u.common.ormlite;

import java.util.Date;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.QueryBuilder;

public class ColumnOrder<E extends Entity> implements Order<E> {
	private final String columnName;
	private final boolean ascending;

	public ColumnOrder(final String columnName, final boolean ascending) {
		this.columnName = columnName;
		this.ascending = ascending;
	}

	@Override
	public QueryBuilder<E, Long> orderBy(final QueryBuilder<E, Long> queryBuilder, final Date now) {
		return orderBy(queryBuilder, columnName, ascending, now);
	}

	protected QueryBuilder<E, Long> orderBy(final QueryBuilder<E, Long> queryBuilder, final String columnName, final boolean ascending, final Date now) {
		return queryBuilder.orderBy(columnName, ascending);
	}
}
