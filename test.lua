#!/usr/bin/env lem
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

print("Entering " .. arg[0])

local prettyprint
do
	local write, format, tostring = io.write, string.format, tostring

	function prettyprint(t)
		local widths = {}
		for i = 1, #t[0] do
			widths[i] = 0
		end

		for i = 0, #t do
			local row = t[i]
			for j = 1, #row do
				local value = row[j]
				if value and #value > widths[j] then
					widths[j] = #value
				end
			end
		end

		for i = 1, #widths do
			widths[i] = '%-' .. tostring(widths[i] + 1) .. 's';
		end

		for i = 0, #t do
			local row = t[i]
			for j = 1, #row do
				write(format(widths[j], row[j] or 'NULL'))
			end
			write('\n')
		end
	end
end

local utils    = require 'lem.utils'
local postgres = require 'lem.postgres'

local db = assert(postgres.connect("host=localhost dbname=mydb"))

assert(db:exec(
'CREATE TABLE mytable (id integer PRIMARY KEY, name CHARACTER VARYING)'))

assert(db:exec("COPY mytable FROM STDIN (delimiter ',')"))
assert(db:put('1,alpha\n'))
assert(db:put('2,beta\n'))
assert(db:put('3,gamma\n'))
assert(db:put('4,delta\n'))
assert(db:done())

local res = assert(db:exec('SELECT * FROM mytable WHERE id = $1', '1'))
prettyprint(res)

local res1, res2 = assert(db:exec([[
SELECT count(id) FROM mytable;
SELECT * FROM mytable WHERE id = 2;
]]));
prettyprint(res2)

assert(db:prepare('mystmt', 'SELECT * FROM mytable WHERE id = $1'))
local res = assert(db:run('mystmt', '3'))
prettyprint(res)

assert(db:exec("COPY mytable TO STDIN (format csv)"))

print("\nDumping CSV:")
while true do
	local row = assert(db:get())
	if type(row) ~= 'string' then break end

	io.write(row)
end

assert(db:exec('DROP TABLE mytable'))

print("Exiting " .. arg[0])

-- vim: syntax=lua ts=2 sw=2 noet:
