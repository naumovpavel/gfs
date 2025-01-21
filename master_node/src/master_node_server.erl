-module(master_node_server).

-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([register_blocks/2, complete_file/2, assign/1, read/3]).

-record(state, {next_block = 1, file_blocks = #{}, block_host = #{}, host_blocks = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

register_blocks(Host, Blocks) ->
    gen_server:cast(?MODULE, {register, Host, Blocks}).

complete_file(File, Blocks) ->
    gen_server:call(?MODULE, {complete, File, Blocks}).

assign(Size) ->
    gen_server:call(?MODULE, {assign, Size}).

read(File, Size, Offset) ->
    gen_server:call(?MODULE, {read, File, Size, Offset}).

handle_call({complete, File, Blocks}, _From, State) ->
    CurrentBlocks = maps:get(File, State#state.file_blocks, []),
    NewBlocks = CurrentBlocks ++ Blocks,
    NewFileBlocks = maps:put(File, NewBlocks, State#state.file_blocks),
    NewState = State#state{file_blocks = NewFileBlocks},
    {reply, ok, NewState};
handle_call({assign, Size}, _From, State) ->
    BlockCount = (Size + 7) div 8,
    {BlockAssignments, NewState} = allocate(State, BlockCount),
    io:format("assign for size ~p:~n", [Size]),
    lists:map(fun ({Host, Block}) -> io:format("    host ~s block ~p~n", [Host, Block]) end, BlockAssignments),
    {reply, BlockAssignments, NewState};
handle_call({read, File, Size, Offset}, _From, State) ->
    case maps:get(File, State#state.file_blocks, undefined) of
        undefined ->
            {reply, {error, file_not_found}, State};
        FileBlocks ->
            FirstBlockIndex = Offset div 8,
            FirstBlockOffset = Offset rem 8,
            FirstBlockSize = min(Size, 8 - FirstBlockOffset),
            RemainingSize = Size - FirstBlockSize,
            BlocksCnt = (RemainingSize + 7) div 8,

            case lists:nth(FirstBlockIndex + 1, FileBlocks) of
                undefined ->
                    {reply, {error, block_not_found}, State};
                FirstBlock ->
                    FirstBlockHost = maps:get(FirstBlock, State#state.block_host),

                    Result =
                        case RemainingSize =< 0 of
                            true ->
                                [{FirstBlockHost, FirstBlock, FirstBlockSize, FirstBlockOffset}];
                            false ->
                                LastBlock = lists:nth(FirstBlockIndex + 1 + BlocksCnt, FileBlocks),
                                [{FirstBlockHost, FirstBlock, FirstBlockSize, FirstBlockOffset}]
                                ++ [{maps:get(Block, State#state.block_host), Block, 8, 0}
                                    || Block
                                           <- middle_blocks(FileBlocks, FirstBlockIndex, BlocksCnt)]
                                ++ [{maps:get(LastBlock, State#state.block_host),
                                     LastBlock,
                                     last_block_size(RemainingSize, 8),
                                     0}]
                        end,
                    {reply, Result, State}
            end
    end.

middle_blocks(FileBlocks, FirstBlockIndex, BlocksCnt) when BlocksCnt >= 2 ->
    lists:sublist(FileBlocks, FirstBlockIndex + 2, BlocksCnt - 1);
middle_blocks(_, _, BlocksCnt) when BlocksCnt < 2 ->
    [].

last_block_size(RemainingSize, Rem) when RemainingSize rem Rem > 0 ->
    RemainingSize rem Rem;
last_block_size(RemainingSize, Rem) when RemainingSize rem Rem < 1 ->
    RemainingSize.

handle_cast({register, Host, Blocks}, State) ->
    MaxBlock = lists:max([0] ++ Blocks),
    NewNextBlock = max(State#state.next_block, MaxBlock + 1),

    NewBlockHost =
        lists:foldl(fun(Block, Acc) -> maps:put(Block, Host, Acc) end,
                    State#state.block_host,
                    Blocks),

    NewHostBlocks = maps:put(Host, Blocks, State#state.host_blocks),
    io:format("new host ~s with blocks ~w~n", [Host, Blocks]),
    NewState =
        State#state{next_block = NewNextBlock,
                    block_host = NewBlockHost,
                    host_blocks = NewHostBlocks},
    {noreply, NewState}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

allocate(State, 0) ->
    {[], State};
allocate(State, BlocksCnt) ->
    FirstBlock = State#state.next_block,
    LastBlock = FirstBlock + BlocksCnt - 1,
    BlockIds = lists:seq(FirstBlock, LastBlock),

    {Assignments, FinalHostBlocks, FinalBlockHost} =
        lists:foldl(fun(BlockId, {Acc, HostBlocks, BlockHost}) ->
                       SortedHosts =
                           lists:sort(fun({_, BlocksA}, {_, BlocksB}) ->
                                         length(BlocksA) < length(BlocksB)
                                      end,
                                      maps:to_list(HostBlocks)),

                       [{FirstHost, _} | RestHosts] = SortedHosts,

                       CurrentSizes =
                           maps:from_list([{Host, length(Blocks)}
                                           || {Host, Blocks} <- SortedHosts]),

                       SelectedHost = select_host(Acc, FirstHost, RestHosts, CurrentSizes),
                       CurrentBlocks = maps:get(SelectedHost, HostBlocks),
                       NewHostBlocks =
                           maps:put(SelectedHost, CurrentBlocks ++ [BlockId], HostBlocks),
                       NewBlockHost = maps:put(BlockId, SelectedHost, BlockHost),

                       {[{SelectedHost, BlockId} | Acc], NewHostBlocks, NewBlockHost}
                    end,
                    {[], State#state.host_blocks, State#state.block_host},
                    BlockIds),

    NewState =
        State#state{next_block = LastBlock + 1,
                    host_blocks = FinalHostBlocks,
                    block_host = FinalBlockHost},

    {lists:reverse(Assignments), NewState}.

select_host(Acc, FirstHost, RestHosts, CurrentSizes) ->
    case Acc of
        [] ->
            FirstHost;
        _ ->
            [{PrevHost, _} | _] = Acc,
            PrevSize = maps:get(PrevHost, CurrentSizes),
            case RestHosts of
                [] ->
                    PrevHost;
                [{NextHost, _} | _] ->
                    NextSize = maps:get(NextHost, CurrentSizes),
                    case PrevSize < NextSize of
                        true ->
                            PrevHost;
                        false ->
                            FirstHost
                    end
            end
    end.

allocate_test_() ->
    [{"allocate with empty host_blocks",
      fun() ->
         State =
             #state{next_block = 1,
                    host_blocks = #{"host1" => [], "host2" => []},
                    block_host = #{}},
         {Assignments, NewState} = allocate(State, 3),
         ?assertEqual([{"host2", 1}, {"host1", 2}, {"host2", 3}], Assignments),
         ?assertEqual(4, NewState#state.next_block),
         ?assertEqual(#{"host1" => [2], "host2" => [1, 3]}, NewState#state.host_blocks),
         ?assertEqual(#{1 => "host2",
                        2 => "host1",
                        3 => "host2"},
                      NewState#state.block_host)
      end},
     {"allocate with existing blocks",
      fun() ->
         State =
             #state{next_block = 10,
                    host_blocks =
                        #{"host1" => [1, 2],
                          "host2" => [3],
                          "host3" => [4, 5, 6]},
                    block_host =
                        #{1 => "host1",
                          2 => "host1",
                          3 => "host2",
                          4 => "host3",
                          5 => "host3",
                          6 => "host3"}},
         {Assignments, NewState} = allocate(State, 4),
         ?assertEqual([{"host2", 10}, {"host2", 11}, {"host1", 12}, {"host3", 13}], Assignments),
         ?assertEqual(14, NewState#state.next_block),
         ?assertEqual(#{"host1" => [1, 2, 12],
                        "host2" => [3, 10, 11],
                        "host3" => [4, 5, 6, 13]},
                      NewState#state.host_blocks)
      end},
     {"allocate with single host",
      fun() ->
         State =
             #state{next_block = 1,
                    host_blocks = #{"host1" => []},
                    block_host = #{}},
         {Assignments, NewState} = allocate(State, 3),
         ?assertEqual([{"host1", 1}, {"host1", 2}, {"host1", 3}], Assignments),
         ?assertEqual(4, NewState#state.next_block),
         ?assertEqual(#{"host1" => [1, 2, 3]}, NewState#state.host_blocks)
      end},
     {"allocate with uneven initial distribution",
      fun() ->
         State =
             #state{next_block = 7,
                    host_blocks =
                        #{"host1" => [1, 2, 3],
                          "host2" => [],
                          "host3" => [4, 5]},
                    block_host =
                        #{1 => "host1",
                          2 => "host1",
                          3 => "host1",
                          4 => "host3",
                          5 => "host3"}},
         {Assignments, NewState} = allocate(State, 5),
         ?assertEqual([{"host2", 7}, {"host2", 8}, {"host3", 9}, {"host2", 10}, {"host3", 11}],
                      Assignments),
         ?assertEqual(12, NewState#state.next_block)
      end},
     {"allocate zero blocks",
      fun() ->
         State =
             #state{next_block = 1,
                    host_blocks = #{"host1" => [], "host2" => []},
                    block_host = #{}},
         {Assignments, NewState} = allocate(State, 0),
         ?assertEqual([], Assignments),
         ?assertEqual(1, NewState#state.next_block),
         ?assertEqual(State#state.host_blocks, NewState#state.host_blocks),
         ?assertEqual(State#state.block_host, NewState#state.block_host)
      end}].
