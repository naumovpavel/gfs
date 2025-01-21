-module(tcp_server).

-export([start/2]).

start(Port, DataDir) ->
    {ok, ListenSocket} =
        gen_tcp:listen(Port, [binary, {active, false}, {reuseaddr, true}, {packet, raw}]),
    accept(ListenSocket, DataDir).

accept(ListenSocket, DataDir) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    spawn(fun() -> handle_client(Socket, DataDir) end),
    accept(ListenSocket, DataDir).

handle_client(Socket, DataDir) ->
    case read_message(Socket) of
        {ok, Data} ->
            Term = binary_to_term(Data),
            Response = handle_request(Term, DataDir),
            ResponseBin = term_to_binary(Response),
            send_message(Socket, ResponseBin),
            handle_client(Socket, DataDir);
        {error, closed} ->
            ok;
        {error, _Reason} ->
            ok
    end.

read_message(Socket) ->
    case gen_tcp:recv(Socket, 4) of
        {ok, <<Size:32>>} ->
            case gen_tcp:recv(Socket, Size) of
                {ok, Payload} ->
                    {ok, Payload};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

send_message(Socket, Data) ->
    Size = byte_size(Data),
    gen_tcp:send(Socket, <<Size:32, Data/binary>>).

handle_request({write, BlockID, Data}, DataDir) ->
    Filename = filename:join([DataDir, integer_to_list(BlockID)]),
    io:format("write new block: id = ~p, data = ~s~n", [BlockID, Data]),
    ok = file:write_file(Filename, Data),
    ok;
handle_request({read, BlockID, Offset, Size}, DataDir) ->
    Filename = filename:join([DataDir, integer_to_list(BlockID)]),
    {ok, File} = file:open(Filename, [read, binary]),
    {ok, _} = file:position(File, Offset),
    {ok, Data} = file:read(File, Size),
    io:format("read block id = ~p, offset = ~p, size = ~p, data = ~s~n",
              [BlockID, Offset, Size, Data]),
    ok = file:close(File),
    Data.
