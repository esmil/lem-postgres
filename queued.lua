--
-- This file is part of lem-postgres.
-- Copyright 2011 Emil Renner Berthing
--
-- lem-postgres is free software: you can redistribute it and/or
-- modify it under the terms of the GNU General Public License as
-- published by the Free Software Foundation, either version 3 of
-- the License, or (at your option) any later version.
--
-- lem-postgres is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with lem-postgres. If not, see <http://www.gnu.org/licenses/>.
--

local utils    = require 'lem.utils'
local postgres = require 'lem.postgres'

local QConnection = {}
QConnection.__index = QConnection

function QConnection:close()
	local ok, err = self.conn:close()
	for i = 1, self.n - 1 do
		self[i]:wakeup()
		self[i] = nil
	end
	return ok, err
end
QConnection.__gc = QConnection.close

do
	local remove = table.remove
	local newsleeper = utils.sleeper

	local function lock(self)
		local n = self.n
		if n == 0 then
			self.n = 1
		else
			local sleeper = newsleeper()
			self[n] = sleeper
			self.n = n + 1
			sleeper:sleep()
		end
		return self.conn
	end
	QConnection.lock = lock

	local function unlock(self, ...)
		local n = self.n - 1
		self.n = n
		if n > 0 then
			remove(self, 1):wakeup()
		end
		return ...
	end
	QConnection.unlock = unlock

	local function wrap(method)
		return function(self, ...)
			return unlock(self, method(lock(self), ...))
		end
	end

	local Connection = postgres.Connection
	QConnection.exec    = wrap(Connection.exec)
	QConnection.prepare = wrap(Connection.prepare)
	QConnection.run     = wrap(Connection.run)
end

local qconnect
do
	local setmetatable = setmetatable
	local connect = postgres.connect

	function qconnect(...)
		local conn, err = connect(...)
		if not conn then return nil, err end

		return setmetatable({
			n = 0,
			conn = conn,
		}, QConnection)
	end
end

return {
	QConnection = QConnection,
	connect = qconnect,
}

-- vim: ts=2 sw=2 noet:
