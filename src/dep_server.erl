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
-export([add_dep/2, add_block_dep/2, remove_dep/2, remove_entry/1, specula_commit/2, set_length/1]).

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

add_block_dep(WriterTxId, Value) ->
    gen_server:cast(dep_server, {add_block_dep, WriterTxId, Value}).

remove_dep(TxId, SCList) ->
    gen_server:cast(dep_server, {remove_dep, TxId, SCList}).

remove_entry(TxId) ->
    gen_server:cast(dep_server, {remove_entry, TxId}).

set_length(Length) ->
    gen_server:cast(dep_server, {set_length, Length}).

specula_commit(PrevTxn, MyTxn) ->
    gen_server:cast(dep_server, {specula_commit, PrevTxn, MyTxn}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([]) ->
    {ok, #state{max_len=0}}.

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({add_block_dep, WriterTxId, {Reader, Value}=BlockedValue}, SD0=#state{max_len=MaxLen}) ->
    case ets:lookup(dep_len, WriterTxId) of
        [] ->
            lager:warning("The dep  to ~w is invalid, writer aborted!", [WriterTxId]),
            local_cert_util:reply(Reader, Value);
        [{WriterTxId, Len, BlockedRead, DepList}] ->
            case Len > MaxLen of
                true ->
                    lager:warning("~w: Adding dep to ~w!", [Reader, WriterTxId]),
                    ets:insert(dep_len, {WriterTxId, Len, [BlockedValue|BlockedRead], DepList});
                false ->
                    local_cert_util:reply(Reader, Value)
            end
    end,
    {noreply, SD0};

handle_cast({specula_commit, PrevTxn, MyTxn}, SD0) ->
    lager:warning("Specula commit for ~w", [MyTxn]),
    AntiDeps = ets:lookup(anti_dep, MyTxn), 
    ets:delete(anti_dep, MyTxn),
    case ets:lookup(dep_len, MyTxn) of
        [] ->
            {Len0, DepList0} = lists:foldl(fun({_, T}, {ML, L}) ->
                                case ets:lookup(dep_len, T) of
                                    [] -> {ML, L};
                                    [{T, Len, _, _}] -> {max(ML,Len+1), [{T, Len}|L]}
                                end end, {0, []}, AntiDeps),
            {MaxLen, DepList} = case PrevTxn of
                                    ignore -> {Len0, DepList0};
                                    _ -> case ets:lookup(dep_len, PrevTxn) of
                                            [] -> {Len0, DepList0};
                                            [{PrevTxn, Len1, _, _}] -> 
                                                ets:insert(dependency, {PrevTxn, MyTxn}),
                                                {max(Len0,Len1+1), [{PrevTxn, Len1}|DepList0]}
                                         end
                                end,
            ets:insert(dep_len, {MyTxn, MaxLen, [], DepList})
    end,
    {noreply, SD0};

handle_cast({set_length, Length}, SD0) ->
    {noreply, SD0#state{max_len=Length}};

handle_cast({remove_entry, TxId}, SD0) ->
    lager:warning("Removing entry for ~w", [TxId]),
    ets:delete(anti_dep, TxId),
    case ets:lookup(dep_len, TxId) of
        [{TxId, _Len1, BlockedRead, _}] ->
            lists:foreach(fun({Reader, Value}) -> 
                local_cert_util:reply(Reader, Value)
                end, BlockedRead),
            ets:delete(dep_len, TxId)
    end,
    {noreply, SD0};

handle_cast({remove_dep, TxId, List}, SD0=#state{max_len=MaxLen}) ->
    lager:warning("Removing dep, TxId is ~w, list is ~w", [TxId, List]),
    AffectedCoord = recursive_reduce_dep([{0, List}], MaxLen, sets:new()),
    ets:delete(anti_dep, TxId),
    lager:warning("Here!!!!"),
    case ets:lookup(dep_len, TxId) of
        [{TxId, _Len1, BlockedRead, _}] ->
            lists:foreach(fun({Reader, Value}) ->
                local_cert_util:reply(Reader, Value)
                end, BlockedRead),
            ets:delete(dep_len, TxId);
        [] -> ok
    end,
    MyClientCoord = {TxId#tx_id.server_pid, TxId#tx_id.client_pid},
    lists:foreach(fun({ClientId, ServerId}=ToCall) ->
        case ToCall of
            MyClientCoord -> ok;
            _ -> gen_server:cast(ServerId, {try_specula_commit, ClientId})
        end
    end, sets:to_list(AffectedCoord)),
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

recursive_reduce_dep([], _MaxLen, OACoord) ->
    OACoord;
recursive_reduce_dep(FullDepList, MaxLen, OACoord) ->
    {NewList, AffectedCoord}= lists:foldl(
        fun({MyLen, DepList}, {Acc, AccCoord}) ->
           lager:warning("DepList is ~w, len is ~w", [DepList, MyLen]),
            lists:foldl(fun({TxId, DepTxId}, {Acc1, AccCoord1}) ->
                R = ets:lookup(dep_len, DepTxId),
                lager:warning("Trying to look for list for ~w, entry is ~w", [DepTxId, R]),
                case R of
                    [] -> 
                        lager:warning("Here!"),
                        {Acc1, AccCoord1}; %% Mean it has been aborted already...
                    [{DepTxId, Len, BlockedValue, DependingList}]= _Entry ->
                        lager:warning("MyLen is ~w, entry is ~w", [MyLen, _Entry]),
                        case MyLen > Len of
                            true -> {Acc1, AccCoord1};
                            false -> 
                                case remove_from_dep_list(DependingList, TxId, MyLen, 0, []) of
                                    [] -> 
                                        lager:warning("Can not remove ~w from dep list ~w for ~w", [TxId, DependingList, DepTxId]),
                                        {Acc1, AccCoord1};
                                    {NewLen, NewList} ->
                                        lager:warning("For ~w: NewLen is ~w, NewList is ~w", [DepTxId, NewLen, NewList]),
                                        case Len > MaxLen of
                                            true ->
                                                ets:insert(dep_len, {DepTxId, NewLen, BlockedValue, NewList});
                                            false ->
                                                lists:foreach(fun({Reader, Value}) -> local_cert_util:reply(Reader, Value)
                                                            end, BlockedValue),
                                                ets:insert(dep_len, {DepTxId, NewLen, [], NewList})
                                        end,
                                        case ets:lookup(dependency, DepTxId) of
                                            [] -> {Acc1, sets:add_element({DepTxId#tx_id.client_pid, DepTxId#tx_id.server_pid}, AccCoord1)};
                                            L -> {[{NewLen+1, L}]++Acc1,
                                                    sets:add_element({DepTxId#tx_id.client_pid, DepTxId#tx_id.server_pid}, AccCoord1)}
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
    lager:warning("So dep is concurrently removed, I'd better do nothing"),
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
