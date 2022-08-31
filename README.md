 --设定当前index
    es:set_index("toby_test_index")
    local ok, err
    --创建这个index
    local ok, err = es:create_index()
    --删除这个index
    local ok, err = es:drop_index()
    --插入/更新doc
    for i=1, 16 do
        ok, err = es:insert_update('uuid_bbbbbbbb234567'..tostring(i), {
            name = 'amy',
            age = 30+i,
            build_time = '2022-8-31 10:23:32',
            info = {
                a = i,
                b = 'b'
            }
        })
    end

    --删除doc
    local ok, err = es:delete('uuid_abcdefg1234567')

    --count
    ok, err = es:count({query = {
            match_all = {}
          }
    })


    -- 搜索
    local qr = { match = {}}
    qr.match["info.a"] = 8
    -- qr.match["name"] = "toby amy"


    local ok, err = es:search({
        query = qr,
        size = 20,
        -- _source = { 'name', 'age' }
    })


    if ok then
        ngx.say(ok)
    else
        ngx.say(err)
    end
