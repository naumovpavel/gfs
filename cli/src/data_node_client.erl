-module(data_node_client).

-export([connect/1, close/1, write/3, read/4, split_address/1]).

connect(Host) ->
    {IP, Port} = split_address(Host),
    gen_tcp:connect(IP, Port, [binary, {active, false}, {packet, raw}]).

close(Socket) ->
    gen_tcp:close(Socket).

write(Socket, Block, Data) ->
    send_request(Socket, {write, Block, Data}).

read(Socket, BlockID, Offset, Size) ->
    {ok, Data} = send_request(Socket, {read, BlockID, Offset, Size}),
    Data.

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

split_address(Address) ->
    case string:split(Address, ":") of
        [IP, Port] ->
            {IP, list_to_integer(Port)};
        _ ->
            {error, invalid_address}
    end.
