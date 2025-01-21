-module(master_node).

-export([main/1]).

main(_) ->
    io:format("starting server~n"),
    tcp_server:start().
