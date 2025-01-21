-module(tcp_server).

-export([start/0]).

start() ->
    master_node_server:start_link(),
    {ok, ListenSocket} =
        gen_tcp:listen(8787, [binary, {active, false}, {reuseaddr, true}, {packet, raw}]),
    accept(ListenSocket).

accept(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    spawn(fun() -> handle_client(Socket) end),
    accept(ListenSocket).

handle_client(Socket) ->
    case read_message(Socket) of
        {ok, Data} ->
            Term = binary_to_term(Data),
            Response = handle_request(Term),
            ResponseBin = term_to_binary(Response),
            send_message(Socket, ResponseBin),
            handle_client(Socket);
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

handle_request({register, Host, Blocks}) ->
    master_node_server:register_blocks(Host, Blocks),
    ok;
handle_request({assign, Size}) ->
    master_node_server:assign(Size);
handle_request({read, File, Size, Offset}) ->
    master_node_server:read(File, Size, Offset);
handle_request({complete, File, Blocks}) ->
    master_node_server:complete_file(File, Blocks).
