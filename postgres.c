/*
 * This file is part of lem-postgres.
 * Copyright 2011 Emil Renner Berthing
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

struct connection {
	struct ev_io w;
	PGconn *conn;
	lua_State *T;
};

static void
push_error(lua_State *T, PGconn *conn)
{
	const char *msg = PQerrorMessage(conn);
	const char *p;

	for (p = msg; *p != '\n' && *p != '\0'; p++);

	lua_pushnil(T);
	if (p > msg)
		lua_pushlstring(T, msg, p - msg);
	else
		lua_pushliteral(T, "unknown error");
}

static int
connection_gc(lua_State *T)
{
	struct connection *c = lua_touserdata(T, 1);

	if (c->conn == NULL)
		return 0;

	ev_io_stop(EV_G_ &c->w);
	PQfinish(c->conn);
	return 0;
}

static int
connection_close(lua_State *T)
{
	struct connection *c;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	c = lua_touserdata(T, 1);
	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "already closed");
		return 2;
	}

	if (c->T != NULL) {
		lua_pushnil(c->T);
		lua_pushliteral(c->T, "interrupted");
		lem_queue(c->T, 2);
		c->T = NULL;
	}

	ev_io_stop(EV_G_ &c->w);
	PQfinish(c->conn);
	c->conn = NULL;

	lua_pushboolean(T, 1);
	return 1;
}

static void
connect_handler(EV_P_ struct ev_io *w, int revents)
{
	struct connection *c = (struct connection *)w;

	(void)revents;

	ev_io_stop(EV_A_ &c->w);
	switch (PQconnectPoll(c->conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING, socket = %d", PQsocket(c->conn));
		ev_io_set(&c->w, PQsocket(c->conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING, socket = %d", PQsocket(c->conn));
		ev_io_set(&c->w, PQsocket(c->conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		lua_settop(c->T, 0);
		push_error(c->T, c->conn);
		PQfinish(c->conn);
		c->conn = NULL;
		lem_queue(c->T, 2);
		c->T = NULL;
		return;

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		lem_queue(c->T, 1);
		c->T = NULL;
		return;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	ev_io_start(EV_A_ &c->w);
}

static int
connection_connect(lua_State *T)
{
	const char *conninfo = luaL_checkstring(T, 1);
	PGconn *conn;
	ConnStatusType status;
	struct connection *c;

	conn = PQconnectStart(conninfo);
	if (conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "out of memory");
		return 2;
	}

	status = PQstatus(conn);
	if (status == CONNECTION_BAD) {
		lem_debug("CONNECTION_BAD");
		goto error;
	}

	c = lua_newuserdata(T, sizeof(struct connection));
	lua_pushvalue(T, lua_upvalueindex(1));
	lua_setmetatable(T, -2);

	c->conn = conn;

	switch (PQconnectPoll(conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING");
		ev_io_init(&c->w, connect_handler, PQsocket(conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING");
		ev_io_init(&c->w, connect_handler, PQsocket(conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		goto error;

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		c->T = NULL;
		return 1;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	c->T = T;
	ev_io_start(EV_G_ &c->w);

	lua_replace(T, 1);
	lua_settop(T, 1);
	return lua_yield(T, 1);
error:
	push_error(T, conn);
	PQfinish(conn);
	return 2;
}

static int
connection_reset(lua_State *T)
{
	struct connection *c;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	if (PQresetStart(c->conn) != 1)
		goto error;

	c->w.cb = connect_handler;
	switch (PQconnectPoll(c->conn)) {
	case PGRES_POLLING_READING:
		lem_debug("PGRES_POLLING_READING");
		ev_io_set(&c->w, PQsocket(c->conn), EV_READ);
		break;

	case PGRES_POLLING_WRITING:
		lem_debug("PGRES_POLLING_WRITING");
		ev_io_set(&c->w, PQsocket(c->conn), EV_WRITE);
		break;

	case PGRES_POLLING_FAILED:
		lem_debug("PGRES_POLLING_FAILED");
		goto error;

	case PGRES_POLLING_OK:
		lem_debug("PGRES_POLLING_OK");
		return 1;

#ifndef NDEBUG
	case PGRES_POLLING_ACTIVE:
		assert(0);
#endif
	}

	c->T = T;
	ev_io_start(EV_G_ &c->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
error:
	push_error(T, c->conn);
	return 2;
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
error_handler(EV_P_ struct ev_io *w, int revents)
{
	struct connection *c = (struct connection *)w;

	(void)revents;

	if (PQconsumeInput(c->conn) == 0) {
		ev_io_stop(EV_A_ &c->w);
		lua_settop(c->T, 0);
		push_error(c->T, c->conn);
		lem_queue(c->T, 2);
		c->T = NULL;
		return;
	}

	while (!PQisBusy(c->conn)) {
		PGresult *res = PQgetResult(c->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &c->w);
			lem_queue(c->T, 2);
			c->T = NULL;
			return;
		}

		PQclear(res);
	}
}

static void
exec_handler(EV_P_ struct ev_io *w, int revents)
{
	struct connection *c = (struct connection *)w;
	PGresult *res;

	(void)revents;

	if (PQconsumeInput(c->conn) != 1) {
		ev_io_stop(EV_A_ &c->w);
		lua_settop(c->T, 0);
		push_error(c->T, c->conn);
		lem_queue(c->T, 2);
		c->T = NULL;
		return;
	}

	while (!PQisBusy(c->conn)) {
		res = PQgetResult(c->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &c->w);
			lem_debug("returning %d values", lua_gettop(c->T) - 1);
			lem_queue(c->T, lua_gettop(c->T) - 1);
			c->T = NULL;
			return;
		}

		switch (PQresultStatus(res)) {
		case PGRES_EMPTY_QUERY:
			lem_debug("PGRES_EMPTY_QUERY");
			lua_settop(c->T, 0);
			lua_pushnil(c->T);
			lua_pushliteral(c->T, "empty query");
			goto error;

		case PGRES_COMMAND_OK:
			lem_debug("PGRES_COMMAND_OK");
			lua_pushboolean(c->T, 1);
			break;

		case PGRES_TUPLES_OK:
			lem_debug("PGRES_TUPLES_OK");
			push_tuples(c->T, res);
			break;

		case PGRES_COPY_IN:
			lem_debug("PGRES_COPY_IN");
			(void)PQsetnonblocking(c->conn, 1);
		case PGRES_COPY_OUT:
			lem_debug("PGRES_COPY_OUT");
			PQclear(res);
			lua_pushboolean(c->T, 1);
			lem_queue(c->T, lua_gettop(c->T) - 1);
			c->T = NULL;
			return;

		case PGRES_BAD_RESPONSE:
			lem_debug("PGRES_BAD_RESPONSE");
			lua_settop(c->T, 0);
			push_error(c->T, c->conn);
			goto error;

		case PGRES_NONFATAL_ERROR:
			lem_debug("PGRES_NONFATAL_ERROR");
			break;

		case PGRES_FATAL_ERROR:
			lem_debug("PGRES_FATAL_ERROR");
			lua_settop(c->T, 0);
			push_error(c->T, c->conn);
			goto error;

		default:
			lem_debug("unknown result status");
			lua_settop(c->T, 0);
			lua_pushnil(c->T);
			lua_pushliteral(c->T, "unknown result status");
			goto error;
		}

		PQclear(res);
	}

	lem_debug("busy");
	return;
error:
	PQclear(res);
	c->w.cb = error_handler;
	while (!PQisBusy(c->conn)) {
		res = PQgetResult(c->conn);

		if (res == NULL) {
			ev_io_stop(EV_A_ &c->w);
			lem_queue(c->T, 2);
			c->T = NULL;
			return;
		}

		PQclear(res);
	}
}

static int
connection_exec(lua_State *T)
{
	struct connection *c;
	const char *command;
	int n;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	command = luaL_checkstring(T, 2);

	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	n = lua_gettop(T) - 2;
	if (n > 0) {
		const char **values = lem_xmalloc(n * sizeof(char *));
		int *lengths = lem_xmalloc(n * sizeof(int));
		int i;

		for (i = 0; i < n; i++) {
			size_t len;

			values[i] = lua_tolstring(T, i+3, &len);
			lengths[i] = len;
		}

		n = PQsendQueryParams(c->conn, command, n,
		                      NULL, values, lengths, NULL, 0);
		free(values);
		free(lengths);
	} else
		n = PQsendQuery(c->conn, command);

	if (n != 1) {
		lem_debug("PQsendQuery failed");
		push_error(T, c->conn);
		return 2;
	}

	c->T = T;
	c->w.cb = exec_handler;
	c->w.events = EV_READ;
	ev_io_start(EV_G_ &c->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static int
connection_prepare(lua_State *T)
{
	struct connection *c;
	const char *name;
	const char *query;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	name = luaL_checkstring(T, 2);
	query = luaL_checkstring(T, 3);

	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	if (PQsendPrepare(c->conn, name, query, 0, NULL) != 1) {
		push_error(T, c->conn);
		return 2;
	}

	c->T = T;
	c->w.cb = exec_handler;
	c->w.events = EV_READ;
	ev_io_start(EV_G_ &c->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static int
connection_run(lua_State *T)
{
	struct connection *c;
	const char *name;
	const char **values;
	int *lengths;
	int n;
	int i;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	name = luaL_checkstring(T, 2);

	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	n = lua_gettop(T) - 2;
	values = lem_xmalloc(n * sizeof(char *));
	lengths = lem_xmalloc(n * sizeof(int));

	for (i = 0; i < n; i++) {
		size_t len;

		values[i] = lua_tolstring(T, i+3, &len);
		lengths[i] = len;
	}

	n = PQsendQueryPrepared(c->conn, name, n,
	                        values, lengths, NULL, 0);
	free(values);
	free(lengths);
	if (n != 1) {
		push_error(T, c->conn);
		return 2;
	}

	c->T = T;
	c->w.cb = exec_handler;
	c->w.events = EV_READ;
	ev_io_start(EV_G_ &c->w);

	lua_settop(T, 1);
	return lua_yield(T, 1);
}

static void
put_handler(EV_P_ struct ev_io *w, int revents)
{
	struct connection *c = (struct connection *)w;
	size_t len;
	const char *data;

	(void)revents;

	data = lua_tolstring(c->T, 2, &len);
	switch (PQputCopyData(c->conn, data, (int)len)) {
	case 1: /* data sent */
		lem_debug("data sent");
		ev_io_stop(EV_A_ &c->w);

		lua_settop(c->T, 0);
		lua_pushboolean(c->T, 1);
		lem_queue(c->T, 1);
		c->T = NULL;
		break;

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		lua_settop(c->T, 0);
		push_error(c->T, c->conn);
		lem_queue(c->T, 2);
		c->T = NULL;
		break;
	}
}

static int
connection_put(lua_State *T)
{
	struct connection *c;
	size_t len;
	const char *data;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	data = luaL_checklstring(T, 2, &len);

	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	switch (PQputCopyData(c->conn, data, (int)len)) {
	case 1: /* data sent */
		lem_debug("data sent");
		lua_pushboolean(T, 1);
		return 1;

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		push_error(T, c->conn);
		return 2;
	}

	c->T = T;
	c->w.cb = put_handler;
	c->w.events = EV_WRITE;
	ev_io_start(EV_G_ &c->w);

	lua_settop(T, 2);
	return lua_yield(T, 2);
}

static void
done_handler(EV_P_ struct ev_io *w, int revents)
{
	struct connection *c = (struct connection *)w;
	const char *error = lua_tostring(c->T, 2);

	(void)revents;

	switch (PQputCopyEnd(c->conn, error)) {
	case 1: /* data sent */
		lem_debug("data sent");
		ev_io_stop(EV_A_ &c->w);
		lua_settop(c->T, 1);
		(void)PQsetnonblocking(c->conn, 0);
		c->w.cb = exec_handler;
		c->w.events = EV_READ;
		ev_io_start(EV_A_ &c->w);
		break;

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		ev_io_stop(EV_A_ &c->w);
		lua_settop(c->T, 0);
		push_error(c->T, c->conn);
		lem_queue(c->T, 2);
		c->T = NULL;
		break;
	}
}

static int
connection_done(lua_State *T)
{
	struct connection *c;
	const char *error;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	error = luaL_optstring(T, 2, NULL);

	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	switch (PQputCopyEnd(c->conn, error)) {
	case 1: /* data sent */
		lem_debug("data sent");
		(void)PQsetnonblocking(c->conn, 0);
		c->T = T;
		c->w.cb = exec_handler;
		c->w.events = EV_READ;
		ev_io_start(EV_G_ &c->w);
		lua_settop(c->T, 1);
		return lua_yield(c->T, 1);

	case 0: /* would block */
		lem_debug("would block");
		break;

	default: /* should be -1 for error */
		push_error(T, c->conn);
		return 2;
	}

	c->T = T;
	c->w.cb = done_handler;
	c->w.events = EV_WRITE;
	ev_io_start(EV_G_ &c->w);

	if (error == NULL) {
		lua_settop(T, 1);
		lua_pushnil(T);
	} else
		lua_settop(T, 2);
	return lua_yield(T, 2);
}

static void
get_handler(EV_P_ struct ev_io *w, int revents)
{
	struct connection *c = (struct connection *)w;
	char *buf;
	int ret;

	ret = PQgetCopyData(c->conn, &buf, 1);
	if (ret > 0) {
		lem_debug("got data");
		ev_io_stop(EV_A_ &c->w);

		lua_pushlstring(c->T, buf, ret);
		PQfreemem(buf);
		lem_queue(c->T, 1);
		c->T = NULL;
		return;
	}

	switch (ret) {
	case 0: /* would block */
		lem_debug("would block");
		break;

	case -1: /* no more data */
		lem_debug("no more data");
		c->w.cb = exec_handler;
		exec_handler(EV_A_ &c->w, revents);
		break;

	default: /* should be -2 for error */
		ev_io_stop(EV_A_ &c->w);
		lua_settop(c->T, 0);
		push_error(c->T, c->conn);
		lem_queue(c->T, 2);
		c->T = NULL;
		break;
	}
}

static int
connection_get(lua_State *T)
{
	struct connection *c;
	char *buf;
	int ret;

	luaL_checktype(T, 1, LUA_TUSERDATA);
	c = lua_touserdata(T, 1);
	if (c->T != NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "busy");
		return 2;
	}

	if (c->conn == NULL) {
		lua_pushnil(T);
		lua_pushliteral(T, "closed");
		return 2;
	}

	ret = PQgetCopyData(c->conn, &buf, 1);
	if (ret > 0) {
		lem_debug("got data");
		lua_pushlstring(T, buf, ret);
		PQfreemem(buf);
		return 1;
	}

	switch (ret) {
	case 0: /* would block */
		lem_debug("would block");
		c->T = T;
		c->w.cb = get_handler;
		c->w.events = EV_READ;
		ev_io_start(EV_G_ &c->w);

		lua_settop(T, 1);
		return lua_yield(T, 1);

	case -1: /* no more data */
		lem_debug("no more data");
		break;

	default: /* should be -2 for error */
		push_error(T, c->conn);
		return 2;
	}

	c->T = T;
	c->w.cb = exec_handler;
	c->w.events = EV_READ;
	ev_io_start(EV_G_ &c->w);

	/* TODO: it is necessary but kinda ugly to call
	 * the exec_handler directly from here.
	 * find a better solution... */
	lua_settop(T, 1);
	exec_handler(EV_G_ &c->w, 0);
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
	/* mt.__gc = <connection_gc> */
	lua_pushcfunction(L, connection_gc);
	lua_setfield(L, -2, "__gc");
	/* mt.close = <connection_close> */
	lua_pushcfunction(L, connection_close);
	lua_setfield(L, -2, "close");
	/* mt.connect = <connection_connect> */
	lua_pushvalue(L, -1); /* upvalue 1: Connection */
	lua_pushcclosure(L, connection_connect, 1);
	lua_setfield(L, -3, "connect");
	/* mt.reset = <connection_reset> */
	lua_pushcfunction(L, connection_reset);
	lua_setfield(L, -2, "reset");
	/* mt.exec = <connection_exec> */
	lua_pushcfunction(L, connection_exec);
	lua_setfield(L, -2, "exec");
	/* mt.prepare = <connection_prepare> */
	lua_pushcfunction(L, connection_prepare);
	lua_setfield(L, -2, "prepare");
	/* mt.run = <connection_run> */
	lua_pushcfunction(L, connection_run);
	lua_setfield(L, -2, "run");
	/* mt.put = <connection_put> */
	lua_pushcfunction(L, connection_put);
	lua_setfield(L, -2, "put");
	/* mt.done = <connection_done> */
	lua_pushcfunction(L, connection_done);
	lua_setfield(L, -2, "done");
	/* mt.get = <connection_get> */
	lua_pushcfunction(L, connection_get);
	lua_setfield(L, -2, "get");
	/* set Connection */
	lua_setfield(L, -2, "Connection");

	return 1;
}
