/*
 * This file is part of lem-postgres.
 * Copyright 2011 Emil Renner Berthing
 * Copyright 2013 Asbjørn Sloth Tønnesen
 *
 * lem-postgres is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * lem-postgres is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with lem-postgres. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdlib.h>
#include <assert.h>
#include <lem.h>
#include <libpq-fe.h>

struct db {
	struct ev_io w;
	PGconn *conn;
};

static int
err_closed(lua_State *T)
{
	lua_pushnil(T);
	lua_pushliteral(T, "closed");
	return 2;
}

static int
err_busy(lua_State *T)
{
	lua_pushnil(T);
	lua_pushliteral(T, "busy");
	return 2;
}

static int
err_connection(lua_State *T, PGconn *conn)
{
	const char *msg = PQerrorMessage(conn);
	const char *p;

	for (p = msg; *p != '\n' && *p != '\0'; p++);

	lua_pushnil(T);
	if (p > msg)
		lua_pushlstring(T, msg, p - msg);
	else
		lua_pushliteral(T, "unknown error");
	return 2;
}

static int
db_gc(lua_State *T)
{
	struct db *d = lua_touserdata(T, 1);

	if (d->conn != NULL)
		PQfinish(d->conn);

	return 0;
}

static int
db_close(lua_State *T)
{
	struct db *d;
	lua_State *S;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);

	S = d->w.data;
	if (S != NULL) {
		ev_io_stop(LEM_ &d->w);
		lua_pushnil(S);
		lua_pushliteral(S, "interrupted");
		lem_queue(S, 2);
		d->w.data = NULL;
	}

	PQfinish(d->conn);
	d->conn = NULL;

	lua_pushboolean(T, 1);
	return 1;
}

static void
db_notice_receiver(void *arg, const PGresult *res)
{
	(void)arg;
	(void)res;
	lem_debug("%s", PQresultErrorMessage(res));
}

static void
postgres_connect_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;

	(void)revents;

	ev_io_stop(EV_A_ &d->w);
	switch (PQconnectPoll(d->conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING, socket = %d", PQsocket(d->conn));
		ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING, socket = %d", PQsocket(d->conn));
		ev_io_set(&d->w, PQsocket(d->conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		PQfinish(d->conn);
		d->conn = NULL;
		return;

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		lem_queue(T, 1);
		d->w.data = NULL;
		return;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	ev_io_start(EV_A_ &d->w);
}

static int
postgres_connect(lua_State *T)
{
	const char *conninfo = luaL_checkstring(T, 1);
	PGconn *conn;
	struct db *d;

	conn = PQconnectStart(conninfo);
	if (conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "out of memory");
		return 2;
	}

	if (PQstatus(conn) == CONNECTION_BAD) {
		lem_debug("CONNECTION_BAD");
		goto error;
	}

	lua_settop(T, 0);
	d = lua_newuserdata(T, sizeof(struct db));
	lua_pushvalue(T, lua_upvalueindex(1));
	lua_setmetatable(T, -2);

	d->conn = conn;
	PQsetNoticeReceiver(conn, db_notice_receiver, NULL);

	switch (PQconnectPoll(conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING");
		ev_io_init(&d->w, postgres_connect_cb, PQsocket(conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING");
		ev_io_init(&d->w, postgres_connect_cb, PQsocket(conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		d->conn = NULL;
		goto error;

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		ev_io_init(&d->w, NULL, PQsocket(conn), 0);
		return 1;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	d->w.data = T;
	ev_io_start(LEM_ &d->w);
	return lua_yield(T, 1);
error:
	err_connection(T, conn);
	PQfinish(conn);
	return 2;
}

static void
db_reset_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;

	(void)revents;

	ev_io_stop(EV_A_ &d->w);
	switch (PQresetPoll(d->conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING, socket = %d", PQsocket(d->conn));
		ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING, socket = %d", PQsocket(d->conn));
		ev_io_set(&d->w, PQsocket(d->conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		PQfinish(d->conn);
		d->conn = NULL;
		return;

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		lem_queue(T, 1);
		d->w.data = NULL;
		return;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	ev_io_start(EV_A_ &d->w);
}

static int
db_reset(lua_State *T)
{
	struct db *d;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);
	if (PQresetStart(d->conn) != 1)
		return err_connection(T, d->conn);

	lua_settop(T, 1);
	switch (PQresetPoll(d->conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING");
		ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING");
		ev_io_set(&d->w, PQsocket(d->conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		return err_connection(T, d->conn);

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		return 1;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	d->w.data = T;
	d->w.cb = db_reset_cb;
	ev_io_start(LEM_ &d->w);
	return lua_yield(T, 1);
}

static void
push_tuples(lua_State *T, PGresult *res)
{
	int rows = PQntuples(res);
	int columns = PQnfields(res);
	int i;

	lua_createtable(T, rows, 0);
	for (i = 0; i < rows; i++) {
		int j;

		lua_createtable(T, columns, 0);
		for (j = 0; j < columns; j++) {
			if (PQgetisnull(res, i, j))
				lua_pushnil(T);
			else
				lua_pushlstring(T, PQgetvalue(res, i, j),
				                   PQgetlength(res, i, j));
			lua_rawseti(T, -2, j+1);
		}
		lua_rawseti(T, -2, i+1);
	}

	/* insert column names as "row 0" */
	lua_createtable(T, columns, 0);
	for (i = 0; i < columns; i++) {
		lua_pushstring(T, PQfname(res, i));
		lua_rawseti(T, -2, i+1);
	}
	lua_rawseti(T, -2, 0);
}

static void
db_error_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;

	(void)revents;

	if (PQconsumeInput(d->conn) == 0) {
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		return;
	}

	while (!PQisBusy(d->conn)) {
		PGresult *res = PQgetResult(d->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &d->w);
			lem_queue(T, 2);
			d->w.data = NULL;
			return;
		}

		PQclear(res);
	}
}

static void
db_exec_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	PGresult *res;

	(void)revents;

	if (PQconsumeInput(d->conn) != 1) {
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		return;
	}

	while (!PQisBusy(d->conn)) {
		res = PQgetResult(d->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &d->w);
			lem_debug("returning %d values", lua_gettop(T) - 1);
			lem_queue(T, lua_gettop(T) - 1);
			d->w.data = NULL;
			return;
		}

		switch (PQresultStatus(res)) {
		case PGRES_EMPTY_QUERY:
			lem_debug("PGRES_EMPTY_QUERY");
			lua_settop(T, 0);
			lua_pushnil(T);
			lua_pushliteral(T, "empty query");
			goto error;

		case PGRES_COMMAND_OK:
			lem_debug("PGRES_COMMAND_OK");
			lua_pushboolean(T, 1);
			break;

		case PGRES_TUPLES_OK:
			lem_debug("PGRES_TUPLES_OK");
			push_tuples(T, res);
			break;

		case PGRES_COPY_IN:
			lem_debug("PGRES_COPY_IN");
			(void)PQsetnonblocking(d->conn, 1);
		case PGRES_COPY_OUT:
			lem_debug("PGRES_COPY_OUT");
			PQclear(res);
			lua_pushboolean(T, 1);
			lem_queue(T, lua_gettop(T) - 1);
			d->w.data = NULL;
			return;

		case PGRES_BAD_RESPONSE:
			lem_debug("PGRES_BAD_RESPONSE");
			lua_settop(T, 0);
			err_connection(T, d->conn);
			goto error;

		case PGRES_NONFATAL_ERROR:
			lem_debug("PGRES_NONFATAL_ERROR");
			break;

		case PGRES_FATAL_ERROR:
			lem_debug("PGRES_FATAL_ERROR");
			lua_settop(T, 0);
			err_connection(T, d->conn);
			goto error;

		default:
			lem_debug("unknown result status");
			lua_settop(T, 0);
			lua_pushnil(T);
			lua_pushliteral(T, "unknown result status");
			goto error;
		}

		PQclear(res);
	}

	lem_debug("busy");
	return;
error:
	PQclear(res);
	d->w.cb = db_error_cb;
	while (!PQisBusy(d->conn)) {
		res = PQgetResult(d->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &d->w);
			lem_queue(T, 2);
			d->w.data = NULL;
			return;
		}

		PQclear(res);
	}
}

void
prepare_params(lua_State *T, int n, const char **values, int *lengths)
{
	int i;

	for (i = 0; i < n; i++) {
		size_t len;
		const char *val;

		if (lua_isnil(T, i+3)) {
			val = NULL;
			/* len is ignored by libpq */
		} else {
			val = lua_tolstring(T, i+3, &len);
			if (val == NULL) {
				free(values);
				free(lengths);
				luaL_argerror(T, i+3, "expected nil or string");
				/* unreachable */
			}
		}

		values[i] = val;
		lengths[i] = len;
	}
}

static int
db_exec(lua_State *T)
{
	struct db *d;
	const char *command;
	int n;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	command = luaL_checkstring(T, 2);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	n = lua_gettop(T) - 2;
	if (n > 0) {
		const char **values = lem_xmalloc(n * sizeof(char *));
		int *lengths = lem_xmalloc(n * sizeof(int));

		prepare_params(T, n, values, lengths);

		n = PQsendQueryParams(d->conn, command, n,
		                      NULL, values, lengths, NULL, 0);
		free(values);
		free(lengths);
	} else
		n = PQsendQuery(d->conn, command);

	if (n != 1) {
		lem_debug("PQsendQuery failed");
		return err_connection(T, d->conn);
	}

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static int
db_prepare(lua_State *T)
{
	struct db *d;
	const char *name;
	const char *query;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	name = luaL_checkstring(T, 2);
	query = luaL_checkstring(T, 3);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);
	if (PQsendPrepare(d->conn, name, query, 0, NULL) != 1)
		return err_connection(T, d->conn);

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static int
db_run(lua_State *T)
{
	struct db *d;
	const char *name;
	int n;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	name = luaL_checkstring(T, 2);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	n = lua_gettop(T) - 2;
	if (n > 0) {
		const char **values = lem_xmalloc(n * sizeof(char *));
		int *lengths = lem_xmalloc(n * sizeof(int));

		prepare_params(T, n, values, lengths);

		n = PQsendQueryPrepared(d->conn, name, n,
				values, lengths, NULL, 0);
		free(values);
		free(lengths);
	} else
		n = PQsendQueryPrepared(d->conn, name, 0, NULL, NULL, NULL, 0);

	if (n != 1)
		return err_connection(T, d->conn);

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static void
db_put_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	size_t len;
	const char *data;
	int ret;

	(void)revents;

	data = lua_tolstring(T, 2, &len);
	switch (PQputCopyData(d->conn, data, (int)len)) {
	case 1: /* data sent */
		lem_debug("data sent");
		lua_settop(T, 0);
		lua_pushboolean(T, 1);
		ret = 1;
		break;

	case 0: /* would block */
		lem_debug("would block");
		return;

	default: /* should be -1 for error */
		lem_debug("error");
		lua_settop(T, 0);
		ret = err_connection(T, d->conn);
		break;
	}

	ev_io_stop(EV_A_ &d->w);
	lem_queue(T, ret);
	d->w.data = NULL;
}

static int
db_put(lua_State *T)
{
	struct db *d;
	size_t len;
	const char *data;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	data = luaL_checklstring(T, 2, &len);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	switch (PQputCopyData(d->conn, data, (int)len)) {
	case 1: /* data sent */
		lem_debug("data sent");
		lua_pushboolean(T, 1);
		return 1;

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		return err_connection(T, d->conn);
	}

	d->w.data = T;
	d->w.cb = db_put_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_WRITE);
	ev_io_start(LEM_ &d->w);

	lua_settop(T, 2);
	return lua_yield(T, 2);
}

static void
db_done_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	const char *error = lua_tostring(T, 2);

	(void)revents;

	switch (PQputCopyEnd(d->conn, error)) {
	case 1: /* data sent */
		lem_debug("data sent");
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 1);
		(void)PQsetnonblocking(d->conn, 0);
		d->w.cb = db_exec_cb;
		ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
		ev_io_start(EV_A_ &d->w);
		break;

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		break;
	}
}

static int
db_done(lua_State *T)
{
	struct db *d;
	const char *error;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	error = luaL_optstring(T, 2, NULL);

	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	switch (PQputCopyEnd(d->conn, error)) {
	case 1: /* data sent */
		lem_debug("data sent");
		(void)PQsetnonblocking(d->conn, 0);
		d->w.data = T;
		d->w.cb = db_exec_cb;
		ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
		ev_io_start(LEM_ &d->w);
		lua_settop(T, 1);
		return lua_yield(T, 1);

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		return err_connection(T, d->conn);
	}

	d->w.data = T;
	d->w.cb = db_done_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_WRITE);
	ev_io_start(LEM_ &d->w);

	if (error == NULL) {
		lua_settop(T, 1);
		lua_pushnil(T);
	} else
		lua_settop(T, 2);
	return lua_yield(T, 2);
}

static void
db_get_cb(EV_P_ struct ev_io *w, int revents)
{
	struct db *d = (struct db *)w;
	lua_State *T = d->w.data;
	char *buf;
	int ret;

	ret = PQgetCopyData(d->conn, &buf, 1);
	if (ret > 0) {
		lem_debug("got data");
		ev_io_stop(EV_A_ &d->w);

		lua_pushlstring(T, buf, ret);
		PQfreemem(buf);
		lem_queue(T, 1);
		d->w.data = NULL;
		return;
	}

	switch (ret) {
	case 0: /* would block */
		lem_debug("would block");
		break;

	case -1: /* no more data */
		lem_debug("no more data");
		d->w.cb = db_exec_cb;
		db_exec_cb(EV_A_ &d->w, revents);
		break;

	default: /* should be -2 for error */
		ev_io_stop(EV_A_ &d->w);
		lua_settop(T, 0);
		lem_queue(T, err_connection(T, d->conn));
		d->w.data = NULL;
		break;
	}
}

static int
db_get(lua_State *T)
{
	struct db *d;
	char *buf;
	int ret;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	d = lua_touserdata(T, 1);
	if (d->conn == NULL)
		return err_closed(T);
	if (d->w.data != NULL)
		return err_busy(T);

	ret = PQgetCopyData(d->conn, &buf, 1);
	if (ret > 0) {
		lem_debug("got data");
		lua_pushlstring(T, buf, ret);
		PQfreemem(buf);
		return 1;
	}

	switch (ret) {
	case 0: /* would block */
		lem_debug("would block");
		d->w.data = T;
		d->w.cb = db_get_cb;
		ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
		ev_io_start(LEM_ &d->w);

		lua_settop(T, 1);
		return lua_yield(T, 1);

	case -1: /* no more data */
		lem_debug("no more data");
		break;

	default: /* should be -2 for error */
		return err_connection(T, d->conn);
	}

	d->w.data = T;
	d->w.cb = db_exec_cb;
	ev_io_set(&d->w, PQsocket(d->conn), EV_READ);
	ev_io_start(LEM_ &d->w);

	/* TODO: it is necessary but kinda ugly to call
	 * the db_exec_cb directly from here.
	 * find a better solution... */
	lua_settop(T, 1);
	db_exec_cb(LEM_ &d->w, 0);
	return lua_yield(T, 1);
}

int
luaopen_lem_postgres(lua_State *L)
{
	lua_newtable(L);

	/* create Connection metatable mt */
	lua_newtable(L);
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	/* mt.__gc = <db_gc> */
	lua_pushcfunction(L, db_gc);
	lua_setfield(L, -2, "__gc");
	/* mt.close = <db_close> */
	lua_pushcfunction(L, db_close);
	lua_setfield(L, -2, "close");
	/* mt.reset = <db_reset> */
	lua_pushcfunction(L, db_reset);
	lua_setfield(L, -2, "reset");
	/* mt.exec = <db_exec> */
	lua_pushcfunction(L, db_exec);
	lua_setfield(L, -2, "exec");
	/* mt.prepare = <db_prepare> */
	lua_pushcfunction(L, db_prepare);
	lua_setfield(L, -2, "prepare");
	/* mt.run = <db_run> */
	lua_pushcfunction(L, db_run);
	lua_setfield(L, -2, "run");
	/* mt.put = <db_put> */
	lua_pushcfunction(L, db_put);
	lua_setfield(L, -2, "put");
	/* mt.done = <db_done> */
	lua_pushcfunction(L, db_done);
	lua_setfield(L, -2, "done");
	/* mt.get = <db_get> */
	lua_pushcfunction(L, db_get);
	lua_setfield(L, -2, "get");

	/* connect = <postgres_connect> */
	lua_pushvalue(L, -1); /* upvalue 1: Connection */
	lua_pushcclosure(L, postgres_connect, 1);
	lua_setfield(L, -3, "connect");

	/* set Connection */
	lua_setfield(L, -2, "Connection");

	return 1;
}
