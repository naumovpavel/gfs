-module(master_node_client).

-export([connect/0, close/1, register/3]).

connect() ->
    gen_tcp:connect("127.0.0.1", 8787, [binary, {active, false}, {packet, raw}]).

close(Socket) ->
    gen_tcp:close(Socket).

register(Socket, Host, Blocks) ->
    send_request(Socket, {register, Host, Blocks}).

send_request(Socket, Request) ->
    Data = term_to_binary(Request),
    Size = byte_size(Data),
    case gen_tcp:send(Socket, <<Size:32, Data/binary>>) of
        ok ->
            receive_response(Socket);
        Error ->
            Error
    end.

receive_response(Socket) ->
    case gen_tcp:recv(Socket, 4) of
        {ok, <<Size:32>>} ->
            case gen_tcp:recv(Socket, Size) of
                {ok, Data} ->
                    {ok, binary_to_term(Data)};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.
