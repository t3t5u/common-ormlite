package com.github.t3t5u.common.ormlite;

import java.util.Date;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.ArgumentHolder;
import com.j256.ormlite.stmt.QueryBuilder;

public class RawOrder<E extends Entity> implements Order<E> {
	private final String rawSql;
	private final ArgumentHolder[] args;

	public RawOrder(final String rawSql, final ArgumentHolder... args) {
		this.rawSql = rawSql;
		this.args = args;
	}

	@Override
	public QueryBuilder<E, Long> orderBy(final QueryBuilder<E, Long> queryBuilder, final Date now) {
		return orderBy(queryBuilder, rawSql, args, now);
	}

	protected QueryBuilder<E, Long> orderBy(final QueryBuilder<E, Long> queryBuilder, final String rawSql, final ArgumentHolder[] args, final Date now) {
		return queryBuilder.orderByRaw(rawSql, args);
	}
}
