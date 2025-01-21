-module(cli).

-export([main/1]).

main(Args) ->
    Op = get_arg("--op", Args),
    case Op of
        "copy" ->
            File = get_arg("--file", Args),
            ok = copy:copy(File),
            io:format("copied~n");
        "cat" ->
            File = get_arg("--file", Args),
            Offset = list_to_integer(get_arg("--offset", Args)),
            Size = list_to_integer(get_arg("--size", Args)),
            cat:cat(File, Size, Offset)
    end.

get_arg(Key, Args) ->
    case lists:dropwhile(fun(Arg) -> Arg =/= Key end, Args) of
        [Key, Value | _] ->
            Value;
        _ ->
            undefined
    end.
