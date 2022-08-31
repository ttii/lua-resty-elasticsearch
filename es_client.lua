-- Copyright (C) by toby
-- create by toby 20220831, for elasticsearch 8.3.3
-- https://www.elastic.co/guide/en/elasticsearch/reference/8.3/docs-index_.html

local cjson = require("cjson")
local http = require("resty.http")

local setmetatable = setmetatable
local _M = { _VERSION = '0.02' }
local mt = { __index = _M }

function _M.new(self, host, port)

    -- ngx.say("new:",cjson.encode(opt))
    local p = setmetatable({
        host = host,
        port = port,
        timeout    = 30000,
        eindex     = ''
    }, mt)

    -- ngx.say("p:",cjson.encode(p))
    return p
end

-- index name
function _M.set_index(self, index)
    self.eindex = index
end


--- timeout
function _M.set_timeout(self, timeout)
    self.timeout = timeout
end


function  _M.call(self, etype, http_req)

    local httpc = http.new()
    httpc:set_timeout(self.timeout)
    httpc:connect(self.host, self.port)


    -- ngx.say( cjson.encode(http_req) )

    if nil == http_req["path"] then

        local epath = "/"  ..self.eindex
        if etype and string.len(etype) > 0 then
            epath = epath .. "/" .. etype
        end

        http_req["path"] = epath
    end

    -- ngx.say( cjson.encode(http_req) )

    local res, err = httpc:request(http_req)
    if not res then
        ngx.say("failed to request: ", err)
        -- local err_s  = string.fr
        return nil, err
    end

    local reader = res.body_reader
    local data = ""

    repeat
        local chunk, err = reader(8192)
        if err then
          ngx.log(ngx.ERR, err)
          break
        end

        if chunk then
            data = data..chunk
        end
    until not chunk

    local ok, err = httpc:set_keepalive()
    if not ok then
        ngx.say("failed to set keepalive: ", err)
        return
    end

    return data
end


-- create index
function  _M.create_index(self)

    --local json_data = cjson.encode(data)

    local ok, err = self:call(nil, {
        method = "PUT",
        headers = {["Content-Type"] = "application/json",}
    })

    return ok, err
end

-- drop index
function _M.drop_index(self)
    local ok, err = self:call(nil, { method = "DELETE", })
    return ok, err
end

-- 插入和更新都用这个接口 , insert or update doc to index
function _M.insert_update(self, id, data)

    local json_data = cjson.encode(data)

    local ok, err = self:call('_doc/'..id, {
        method = "POST",
        body = json_data,
        headers = { ["Content-Type"] = "application/json", }
    })

    return ok, err
end

-- delete from index
function _M.delete(self, id)
    local ok, err = self:call('_doc/'.. id, {method = "DELETE"})
    return ok, err
end

-- count all index
function _M.count(self, condition)
    local json_data = cjson.encode(condition)

    local ok, err = self:call('_count', {
        method = "GET",
        body = json_data,
        headers = { ["Content-Type"] = "application/json", }
    })
    return ok, err
end


-- search
function _M.search(self, condition)

    local json_data = cjson.encode(condition)

    local ok, err = self:call('_search',{
        method = "GET",
        body = json_data,
        headers = {["Content-Type"] = "application/json",}
    })
    return ok, err
end



return _M

