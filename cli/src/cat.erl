-module(cat).

-export([cat/3]).

cat(Filename, Size, Offset) ->
    {ok, MasterSocket} = master_node_client:connect(),
    {ok, BlockLocations} = master_node_client:read(MasterSocket, Filename, Size, Offset),
    Data = collect_blocks(BlockLocations, <<>>),
    master_node_client:close(MasterSocket),
    io:format("~s~n", [Data]).

collect_blocks([], Acc) ->
    Acc;
collect_blocks([{Host, BlockID, Size, Offset} | Rest], Acc) ->
    {ok, DataSocket} = data_node_client:connect(Host),
    BlockData = data_node_client:read(DataSocket, BlockID, Offset, Size),
    data_node_client:close(DataSocket),
    collect_blocks(Rest, <<Acc/binary, BlockData/binary>>).
