-module(data_node).

-export([main/1]).

main(Args) ->
    Port = get_arg("--port", Args),
    Host = "localhost:" ++ Port,
    DataDir = get_arg("--data-dir", Args),

    {ok, Files} = file:list_dir(DataDir),
    Blocks = [list_to_integer(filename:basename(F)) || F <- Files],
    {ok, Socket} =
        gen_tcp:connect("localhost", 8787, [binary, {active, false}, {packet, raw}]),
    {ok, ok} = master_node_client:register(Socket, Host, Blocks),
    tcp_server:start(list_to_integer(Port), DataDir).

get_arg(Key, Args) ->
    case lists:dropwhile(fun(Arg) -> Arg =/= Key end, Args) of
        [Key, Value | _] ->
            Value;
        _ ->
            undefined
    end.
