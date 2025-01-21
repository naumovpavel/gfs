-module(copy).

-export([copy/1]).

-include_lib("kernel/include/file.hrl").

copy(Filename) ->
    {ok, MasterSocket} = master_node_client:connect(),
    {ok, FileInfo} = file:read_file_info(Filename),
    FileSize = FileInfo#file_info.size,
    {ok, Assignments} = master_node_client:assign(MasterSocket, FileSize),
    {ok, File} = file:open(Filename, [read, binary]),
    Blocks = process_blocks(File, Assignments, []),
    file:close(File),
    master_node_client:complete(MasterSocket, Filename, Blocks),
    master_node_client:close(MasterSocket),
    ok.

process_blocks(_File, [], BlocksAcc) ->
    lists:reverse(BlocksAcc);
process_blocks(File, [{Host, BlockID} | Rest], BlocksAcc) ->
    case file:read(File, 8) of
        {ok, Data} ->
            {ok, DataSocket} = data_node_client:connect(Host),
            {ok, ok} = data_node_client:write(DataSocket, BlockID, Data),
            data_node_client:close(DataSocket),
            process_blocks(File, Rest, [BlockID | BlocksAcc]);
        eof ->
            lists:reverse(BlocksAcc)
    end.
