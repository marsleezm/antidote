%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(dep_server).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0]).

%% Callbacks
-export([init/1,
	    handle_call/3,
	    handle_cast/2,
        code_change/3,
        handle_event/3,
        handle_info/2,
        handle_sync_event/4,
        terminate/2]).

%% States
-export([add_dep/2, add_block_dep/3, remove_dep/3, specula_commit/2]).

%% Spawn
-record(state, {max_len :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, dep_server},
             ?MODULE, [], []).

add_dep(ReaderTxId, WriterTxId) ->
    gen_server:cast(dep_server, {add_dep, ReaderTxId, WriterTxId}).

add_block_dep(ReaderTxId, WriterTxId, Value) ->
    gen_server:cast(dep_server, {add_block_dep, ReaderTxId, WriterTxId, Value}).

remove_dep(List, SCList, NotReply) ->
    gen_server:cast(dep_server, {remove_dep, List, SCList, NotReply}).

specula_commit(PrevTxn, MyTxn) ->
    gen_server:cast(dep_server, {specula_commit, PrevTxn, MyTxn}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([]) ->
    {ok, #state{max_len=5}}.

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({add_dep, ReaderTxId, WriterTxId}, SD0=#state{max_len=_MaxLen}) ->
    case ets:lookup(dep_len, WriterTxId) of
        [] -> lager:warning("The dep from ~w to ~w is invalid, writer aborted!", [ReaderTxId, WriterTxId]),
            ok;
        [{WriterTxId, Len, spec_commit, _,  _}] ->
            case ets:lookup(dep_len, ReaderTxId) of
                [] -> lager:warning("Reader ~w Aborted! Cancel!", [ReaderTxId]);
                Entry -> 
                    NewEntry = add_to_entry(Entry, ReaderTxId, Len+1, WriterTxId), 
                    lager:warning("Add readerDep :~w, ~w", [Entry, NewEntry]),
                    case NewEntry of
                        {} -> ok;
                   %lager:warning("~p reading, added dep, limit is ~w, new entry is ~w", [ReaderTxId, _MaxLen, NewEntry]),
                        _ -> ets:insert(dep_len, NewEntry) 
                    end
            end
    end,
    {noreply, SD0};

handle_cast({add_block_dep, ReaderTxId, WriterTxId, {Reader, Value}=BlockedValue}, SD0=#state{max_len=MaxLen}) ->
    case ets:lookup(dep_len, WriterTxId) of
        [] ->
            lager:warning("The dep from ~w to ~w is invalid, writer aborted!", [ReaderTxId, WriterTxId]),
            local_cert_util:reply(Reader, Value);
        [{WriterTxId, Len, spec_commit, _, _}] ->
            case ets:lookup(dep_len, ReaderTxId) of
                [] -> lager:warning("Reader ~w Aborted! Cancel!", [ReaderTxId]);
                Entry -> 
                    NewEntry = case Len+1 > MaxLen of
                                   true -> add_to_wait_entry(Entry, ReaderTxId, Len+1, WriterTxId, BlockedValue); 
                                   false -> local_cert_util:reply(Reader, Value),
                                           add_to_entry(Entry, ReaderTxId, Len+1, WriterTxId)
                               end,
                    lager:warning("Add block readerDep :~w, ~w", [Entry, NewEntry]),
                    case NewEntry of
                        {} -> ok;
                        _ -> ets:insert(dep_len, NewEntry)
                    end,
                   lager:warning("~p reading specula ~p, but blocked! LenLimit is ~w, new entry is ~w", [ReaderTxId, Value, MaxLen, NewEntry]),
                    ok
            end
    end,
    {noreply, SD0};

handle_cast({specula_commit, PrevTxn, MyTxn}, SD0) ->
    case ets:lookup(dep_len, MyTxn) of
        [] ->
            ok;
        [{MyTxn, Len, [], 0, DepList}] ->
            case ets:lookup(dep_len, PrevTxn) of
                [] -> 
                    lager:warning("Specula committing ~w, DepList is ~w", [MyTxn, DepList]),
                    ets:insert(dep_len, {PrevTxn, max(1, Len), spec_commit, 1, DepList});
                [{_, PrevLen, _, _, _}] ->
                    lager:warning("Specula committing ~w, DepList is ~w", [MyTxn, DepList]),
                    ets:insert(dep_len, {PrevTxn, max(PrevLen+1, Len), spec_commit, PrevLen+1, DepList})
            end
    end,
    {noreply, SD0};

handle_cast({remove_dep, List, SCList, MyClientCoord}, SD0=#state{max_len=MaxLen}) ->
    lager:warning("Removing dep, list is ~w, sclist is ~w", [List, SCList]),
    ReduceList = reduce_sc_dep(SCList, 0, []),
    AffectedCoord = recursive_reduce_dep([{0, List}|ReduceList], MaxLen, sets:new()),
    case MyClientCoord of
        ignore ->
            lists:foreach(fun({ClientId, ServerId}) ->
                        gen_server:cast(ServerId, {try_specula_commit, ClientId})
                        end, sets:to_list(AffectedCoord));
        _ ->
            lists:foreach(fun({ClientId, ServerId}=ToCall) ->
                case ToCall of
                    MyClientCoord -> ok;
                    _ -> gen_server:cast(ServerId, {try_specula_commit, ClientId})
                end
                end, sets:to_list(AffectedCoord))
    end,
    {noreply, SD0};

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

add_to_entry([], TxId, Len, Key) ->
   %lager:warning("Len is ~w, Key is ~w", [Len, Key]),
    {TxId, Len, [], 0, [{Key, Len}]};
add_to_entry([{TxId, OldLen, WaitValue, SCLen, List}], TxId, Len, Key) ->
   %lager:warning("Len is ~w, Key is ~w", [Len, Key]),
    case add_to_lim_list(List, Key, Len, []) of
        no_need -> {};
        L -> {TxId, max(Len, OldLen), WaitValue, SCLen, L}
    end.

add_to_wait_entry([], TxId, Len, Key, WaitValue) ->
   %lager:warning("Len is ~w, Key is ~w", [Len, Key]),
    {TxId, Len, WaitValue, 0, [{Key, Len}]};
add_to_wait_entry([{TxId, OldLen, _, SCLen, List}], TxId, Len, Key, WaitValue) ->
   %lager:warning("Len is ~w, Key is ~w", [Len, Key]),
    case add_to_lim_list(List, Key, Len, []) of
        no_need -> {};
        L -> {TxId, max(Len, OldLen), WaitValue, SCLen, L}
    end.

add_to_lim_list([], Key, Len, Acc) ->
    [{Key, Len}|Acc];
add_to_lim_list([{Key, MyLen}|Rest], Key, Len, Acc) ->
    case Len =< MyLen of
        true -> no_need;
        false -> [{Key, Len}|Rest] ++ Acc
    end;
add_to_lim_list([H|Rest], Key, Len, Acc) ->
    add_to_lim_list(Rest, Key, Len, [H|Acc]).

reduce_sc_dep([], _MyLen, ReduceList) ->
    ReduceList;
reduce_sc_dep([H|T], MyLen, ReduceList) ->
    case ets:lookup(dep_len, H) of
        [] -> ReduceList;
        [{H, OldLen, BlockedValue, SCLen, DependingList}] -> 
            case MyLen+1 >= SCLen of
                true -> ReduceList;
                false ->
                    case MyLen+1 >= OldLen of
                        true -> ReduceList;
                        false -> 
                            lager:warning("Reduce sc dep for ~w, inserting ~w, depending list is ~w", [H, BlockedValue, DependingList]),
                            ets:insert(dep_len, {H, MyLen+1, BlockedValue, MyLen+1, DependingList}),
                            case ets:lookup(dependency, H) of
                                [] -> reduce_sc_dep(T, MyLen+1, ReduceList); 
                                L -> reduce_sc_dep(T, MyLen+1, [{MyLen+1, L}|ReduceList])
                            end
                    end
            end
    end.

recursive_reduce_dep([], _MaxLen, OACoord) ->
    OACoord;
recursive_reduce_dep(FullDepList, MaxLen, OACoord) ->
    {NewList, AffectedCoord}= lists:foldl(
        fun({MyLen, DepList}, {Acc, AccCoord}) ->
           lager:warning("DepList is ~w, len is ~w", [DepList, MyLen]),
            lists:foldl(fun({TxId, DepTxId}, {Acc1, AccCoord1}) ->
               lager:warning("Trying to look for list for ~w", [DepTxId]),
                case ets:lookup(dep_len, DepTxId) of
                    [] -> {Acc1, AccCoord1}; %% Mean it has been aborted already...
                    [{_DepTxId, _Len, _BlockedValue, _SCLen, []}] -> 
                        lager:warning("Skipping tx id ~w, because no dep, len is ~w", [DepTxId, _Len]), 
                        {Acc1, AccCoord1};
                    [{DepTxId, Len, BlockedValue, SCLen, DependingList}]= Entry ->
                        lager:warning("MyLen is ~w, entry is ~w", [MyLen, Entry]),
                        case MyLen > Len of
                            true -> {Acc1, AccCoord1};
                            false -> 
                                case remove_from_dep_list(DependingList, TxId, MyLen, 0, []) of
                                    [] -> 
                                        lager:warning("Can not remove ~w from dep list ~w for ~w", [TxId, DependingList, DepTxId]),
                                        {Acc1, AccCoord1};
                                    {NewLen, NewList} ->
                                        lager:warning("For ~w: NewLen is ~w, NewList is ~w", [DepTxId, NewLen, NewList]),
                                        case BlockedValue of
                                            spec_commit ->
                                                NewLen1 = max(SCLen, NewLen + 1),
                                                ets:insert(dep_len, {DepTxId, NewLen1, BlockedValue, SCLen, NewList}),
                                                case ets:lookup(dependency, DepTxId) of
                                                    [] -> {Acc1, sets:add_element({DepTxId#tx_id.client_pid, DepTxId#tx_id.server_pid}, AccCoord1)};
                                                    L -> {[{NewLen1, L}]++Acc1,
                                                            sets:add_element({DepTxId#tx_id.client_pid, DepTxId#tx_id.server_pid}, AccCoord1)}
                                                end;
                                            {Reader, Value} ->
                                                SCLen = 0,
                                                case NewLen =< MaxLen of
                                                    true ->
                                                        local_cert_util:reply(Reader, Value),
                                                        ets:insert(dep_len, {DepTxId, NewLen, [], 0, NewList});
                                                    false ->
                                                        ets:insert(dep_len, {DepTxId, NewLen, BlockedValue, 0, NewList})
                                                end,
                                                {Acc1, AccCoord1};
                                            [] ->
                                                SCLen = 0,
                                                ets:insert(dep_len, {DepTxId, NewLen, [], 0, NewList}),
                                                {Acc1, AccCoord1}
                                        end
                                end
                        end
                end end, {Acc, AccCoord}, DepList)
        end, {[], OACoord}, FullDepList),
    recursive_reduce_dep(NewList, MaxLen, AffectedCoord).

remove_from_dep_list([], done, _MyLen, 0, []) ->
    {0, []};
remove_from_dep_list([], done, _MyLen, NewMaxLen, NewList) ->
    {NewMaxLen, NewList};
remove_from_dep_list([], Key, _,  _, _) ->
   %lager:warning("So dep is concurrently removed, I'd better do nothing"),
    Key = [];
remove_from_dep_list([{Key, _Len}|Rest], Key, 0, NewMaxLen, NewList) ->
    remove_from_dep_list(Rest, done, 0, NewMaxLen, NewList);
remove_from_dep_list([{Key, Len}|Rest], Key, MyLen, NewMaxLen, NewList) ->
    case Len > MyLen of
        true -> remove_from_dep_list(Rest, done, MyLen, max(NewMaxLen, MyLen), [{Key, MyLen}|NewList]);
        false -> lager:error("Removing ~w, old len is ~w, new len is ~w", [Key, Len, MyLen]),
                []
    end;
remove_from_dep_list([{OtherKey, Len}|Rest], Key, MyLen, NewMaxLen, NewList) ->
    remove_from_dep_list(Rest, Key, MyLen, max(NewMaxLen, Len), [{OtherKey, Len}|NewList]).
