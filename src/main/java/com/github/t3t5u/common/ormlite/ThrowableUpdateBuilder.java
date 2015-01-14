package com.github.t3t5u.common.ormlite;

import java.sql.SQLException;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.UpdateBuilder;

public interface ThrowableUpdateBuilder<E extends Entity> {
	UpdateBuilder<E, Long> buildUpdate(UpdateBuilder<E, Long> updateBuilder) throws SQLException;
}
