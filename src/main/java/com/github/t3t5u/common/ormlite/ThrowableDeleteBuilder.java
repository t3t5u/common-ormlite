package com.github.t3t5u.common.ormlite;

import java.sql.SQLException;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.DeleteBuilder;

public interface ThrowableDeleteBuilder<E extends Entity> {
	DeleteBuilder<E, Long> buildDelete(DeleteBuilder<E, Long> deleteBuilder) throws SQLException;
}
