------------------------------------------------------------------------------
--                               Require                                    --
------------------------------------------------------------------------------

require('orm.class.global')
require("orm.tools.func")

local Table = require('orm.class.table')

------------------------------------------------------------------------------
--                                Constants                                 --
------------------------------------------------------------------------------
-- Global
ID = "id"
AGGREGATOR = "aggregator"
QUERY_LIST = "query_list"

-- databases types
TYPE_SQLITE = "sqlite3"
TYPE_ORACLE = "oracle"
TYPE_MYSQL = "mysql"
TYPE_POSTGRESQL = "postgresql"

------------------------------------------------------------------------------
--                              Model Settings                              --
------------------------------------------------------------------------------

if not DB then
    BACKTRACE(INFO, "Cna't find global database settings variable 'DB'")
    DB = {}
end

DB = {
    -- ORM settings
    new = (DB.new == true),
    DEBUG = (DB.DEBUG == true),
    backtrace = (DB.backtrace == true),
    -- database settings
    dbtype = DB.dbtype or "sqlite3",
    -- if you use sqlite set database path value
    -- if not set a database name
    dbname = DB.dbname or "database.db",
    -- not sqllite db settings
    host = DB.host or nil,
    port = DB.port or nil,
    username = DB.username or nil,
    password = DB.password or nil
}

local sql, _connect
local luasql 

-- Get database by settings
if DB.dbtype == TYPE_SQLITE then
    luasql = require("luasql.sqlite3")
    sql = luasql.sqlite3()
    _connect = sql:connect(DB.dbname)

elseif DB.dbtype == TYPE_MYSQL then
    luasql = require("luamysql")
    sql = luasql.mysql()
    print(DB.dbname, DB.username, DB.password, DB.host, DB.port)
    _connect = sql:connect(DB.dbname, DB.username, DB.password, DB.host, DB.port)

elseif DB.dbtype == TYPE_POSTGRESQL then
    luasql = require("luasql.postgres")
    sql = luasql.postgres()
    print(DB.dbname, DB.username, DB.password, DB.host, DB.port)
    _connect = sql:connect(DB.dbname, DB.username, DB.password, DB.host, DB.port)

else
    BACKTRACE(ERROR, "Database dbtype not suported '" .. tostring(DB.dbtype) .. "'")
end

if not _connect then
    BACKTRACE(ERROR, "Connect problem!")
end

-- if DB.new then
--     BACKTRACE(INFO, "Remove old database")

--     if DB.dbtype == TYPE_SQLITE then
--         os.remove(DB.dbname)
--     else
--         _connect:execute('DROP DATABASE `' .. DB.dbname .. '`')
--     end
-- end

------------------------------------------------------------------------------
--                               Database                                   --
------------------------------------------------------------------------------

-- Database settings
db = {
    -- Satabase connect instance
    connect = _connect,

    -- Execute SQL query
    execute = function (self, query)
        BACKTRACE(DEBUG, query)
        ---- fixed add err return value catch mysql error 
        local result,err = self.connect:execute(query)
        if result then
            return result
        else
            BACKTRACE(ERROR, "Wrong SQL query " .. tostring(err))
            return result,err
        end
    end,

    -- Return insert query id
    insert = function (self, query)
        local _cursor, err = self:execute(query)
        ---- modify 20160827
        if not _cursor then
            local break_conn = string.find(err, "LuaSQL%:%serror%sexecuting%squery%.%sMySQL%:%sMySQL%sserver%shas%sgone%saway")
            if break_conn then
                assert(false, tostring(err))
            end
            return nil, err
        end

        -- type(_cursor) is number 
        -- _cursor value is effect rows

        return tonumber(_cursor)
    end,

    -- get parced data
    rows = function (self, query, own_table)
        local _cursor = self:execute(query)
        local data = {}
        local current_row = {}
        local current_table
        local row

        if _cursor then
            row = _cursor:fetch({}, "a")

            while row do
                for colname, value in pairs(row) do
                    current_table, colname = string.divided_into(colname, "_")

                    if current_table == own_table.__tablename__ then
                        current_row[colname] = value
                    else
                        if not current_row[current_table] then
                            current_row[current_table] = {}
                        end

                        current_row[current_table][colname] = value
                    end
                end

                table.insert(data, current_row)

                current_row = {}
                row = _cursor:fetch({}, "a")
            end

        end

        return data
    end
}

return Table