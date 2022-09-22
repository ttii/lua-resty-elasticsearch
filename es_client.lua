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

-- get index
function  _M.get_index(self)

    --local json_data = cjson.encode(data)

    local ok, err = self:call(nil, {
        method = "GET",
        headers = {["Content-Type"] = "application/json",}
    })


    return ok, err
end

-- create index
--[=[
       local mapping_properties = {
        -- 哪些字段需要中文全文索引的,需要在这里加
        --创建索引后,需要设置中文搜索引擎的mapping参数
        --[[
            其中分词有两种设置方法，ik_max_word和ik_smart，他们的区别如下，可以根据自己项目的情况进行选择：
            ik_max_word: 会将文本做最细粒度的拆分，比如会将“中华人民共和国国歌”拆分为“中华人民共和国,中华人民,中华,华人,人民共和国,人民,人,民,共和国,共和,和,国国,国歌”，会穷尽各种可能的组合；
            ik_smart: 会做最粗粒度的拆分，比如会将“中华人民共和国国歌”拆分为“中华人民共和国,国歌”。
        ]]
        sku_name = {
            type = "text",
            analyzer = "ik_smart",
            search_analyzer = "ik_smart"
        },
        purpose = {
            type = "short"
        },
        btype = {
            type = "short"
        },
        sko_uuid = {
            type = "keyword",
            ignore_above = 16 --长于这个长度的字符串不会被索引
        },
        memo = {
            type = "object",
            enabled = "false" --这种不做索引,只是数据在里面,从结果中获得得了
        }

    }
]=]
function _M.create_index(self, mapping_properties)
    local ok, err
    -- 如果带有mapping配置,那么走mapping创建,做静态的mapping配置,否则直接创建并走动态的mapping配置
    if mapping_properties then
         -- 创建mapping,参数结构外围需要添加mappings关键字
        local mapjson = {
            mappings = {
                properties = mapping_properties
            }
        }

        local body_str = cjson.encode(mapjson)
        print(body_str)
        ok, err = self:call(nil,{
            method = "PUT",
            body = body_str,
            headers = {["Content-Type"] = "application/json",}
        })

    else
        ok, err = self:call(nil, {
            method = "PUT",
            headers = {["Content-Type"] = "application/json",}
        })
    end

    return ok, err
end

-- drop index
function _M.drop_index(self)
    local ok, err = self:call(nil, { method = "DELETE", })
    return ok, err
end

完全的插入和更新都用这个接口 , insert or update doc to index
意思就是更新的话,整个data会把原有的data替换掉,没有的字段就删掉了,全新的替换
--*如果需要修改某个字段内容,请使用update接口,单独更新某个字段内容
function _M.insert_update(self, id, data)

    local json_data = cjson.encode(data)

    local ok, err = self:call('_doc/'..id, {
        method = "POST",
        body = json_data,
        headers = { ["Content-Type"] = "application/json", }
    })

    return ok, err

--[[
--*单独更新doc的某个字段的内容,data不会覆盖整个文档,只会修改data中涉及到的字段内容
data = { "tag_list": [ "噜啦啦" ]}
]]
function _M.update(self, id, data)
    local docjson = {
        doc = data
    }
    local json_data = cjson.encode(docjson)

    local ok, err = self:call('_update/'..id, {
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



-- add mapping properties
--[=[
       local properties = {
        -- 哪些字段需要中文全文索引的,需要在这里加
        --创建索引后,需要设置中文搜索引擎的mapping参数
        --[[
            其中分词有两种设置方法，ik_max_word和ik_smart，他们的区别如下，可以根据自己项目的情况进行选择：
            ik_max_word: 会将文本做最细粒度的拆分，比如会将“中华人民共和国国歌”拆分为“中华人民共和国,中华人民,中华,华人,人民共和国,人民,人,民,共和国,共和,和,国国,国歌”，会穷尽各种可能的组合；
            ik_smart: 会做最粗粒度的拆分，比如会将“中华人民共和国国歌”拆分为“中华人民共和国,国歌”。
        ]]
        sku_name = {
            type = "text",
            analyzer = "ik_smart",
            search_analyzer = "ik_smart"
        },
        purpose = {
            type = "short"
        },
        btype = {
            type = "short"
        },
        sko_uuid = {
            type = "keyword",
            ignore_above = 16 --长于这个长度的字符串不会被索引
        },
        memo = {
            type = "object",
            enabled = "false" --这种不做索引,只是数据在里面,从结果中获得得了
        }

    }
]=]
function _M.mapping_add(self, properties)

    -- 添加新字段的mapping,外围是properties关键字
    local propjson = {
        properties = properties
    }
    local json_data = cjson.encode(propjson)

    local ok, err = self:call('_mapping',{
        method = "POST",
        body = json_data,
        headers = {["Content-Type"] = "application/json",}
    })
    
    return ok, err
end

-- get mapping
function _M.mapping_get(self)

    local ok, err = self:call('_mapping',{
        method = "GET",
        headers = {["Content-Type"] = "application/json",}
    })
   
    return ok, err
end
return _M

