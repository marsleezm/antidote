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

-module(tx_cert_sup).

-behavior(supervisor).

-include("antidote.hrl").

-export([start_link/0]).

-export([init/1, certify/4, certify_update/6, get_stat/0, get_int_data/3, start_tx/3, single_read/3, clean_all_data/0, clean_data/1, 
            load_local/3, start_read_tx/1, set_int_data/3, read/4, single_commit/4, append_values/4, load/2, trace/1, get_oldest/0,
            get_oldest/1, get_all_size/0, get_size/1, read_all/0, read_all_nodes/0, 
            get_all_oldest/0,  get_pid/1, get_pids/1, get_global_pid/1, get_hitcounters/0]).

-define(READ_TIMEOUT, 30000).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

single_commit(Name, Node, Key, Value) ->
    append_values(Name, Node, [{Key, Value}], tx_utilities:now_microsec()).

append_values(Name, Node, KeyValues, CommitTime) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), 
                    {append_values, Node, KeyValues, CommitTime}, 15000);
        false ->
            gen_server:call(Name, 
                    {append_values, Node, KeyValues, CommitTime}, 15000)
    end.

load(Type, Param) ->
    {PartList, _, _} =  hash_fun:get_hash_fun(), %gen_server:call({global, MyTxServer}, {get_hash_fun}),
    lager:info("Got load request, part list is ~w", [PartList]),
    AllDcs = [N || {N, _} <- PartList],
    StartTime = os:timestamp(),
    lists:foreach(fun(Node) ->
                    lager:info("Asking ~p to load", [Node]),
                    spawn(rpc, call, [Node, tx_cert_sup, load_local, [self(), Type, Param]])
                 end, AllDcs),
    lager:info("Waiting for results..."),
    lists:foreach(fun(_) ->
                 receive done -> ok end,
                lager:info("Received ack")
                 end, AllDcs),
    EndTime = os:timestamp(),
    lager:info("Totally finished in tx_cert_sup, used ~w secs!!!", [timer:now_diff(EndTime, StartTime)/1000000]).

load_local(Sender, Type, Param) ->
    CertServer = list_to_atom(atom_to_list(node()) ++ "-cert-" ++ integer_to_list(1)),
    gen_server:cast(CertServer, {load, self(), Type, Param}),
    receive done -> ok end,
    Sender ! done.

trace(Num) ->
    CertServer = list_to_atom(atom_to_list(node()) ++ "-cert-" ++ integer_to_list(Num)),
    gen_server:cast(CertServer, {trace_pending}).

start_tx(Name, Seq, Client) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), {start_tx, Seq, Client});
        false ->
            gen_server:call(Name, {start_tx, Seq, Client})
    end.

start_read_tx(Name) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), {start_read_tx});
        false ->
            gen_server:call(Name, {start_read_tx})
    end.

certify(Name, TxId, LocalUpdates, RemoteUpdates) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), 
                    {certify, TxId, LocalUpdates, RemoteUpdates});
        false ->
            gen_server:call(Name, 
                    {certify, TxId, LocalUpdates, RemoteUpdates})
    end.

certify_update(Name, TxId, LocalUpdates, RemoteUpdates, MsgId, Client) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), 
                    {certify_update, TxId, LocalUpdates, RemoteUpdates, MsgId, Client});
        false ->
            gen_server:call(Name, 
                    {certify_update, TxId, LocalUpdates, RemoteUpdates, MsgId, Client})
    end.

get_int_data(Name, Type, Param) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), {get_int_data, Type, Param});
        false ->
            gen_server:call(Name, {get_int_data, Type, Param})
    end.

set_int_data(Name, Type, Param) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP +1), {set_int_data, Type, Param});
        false ->
            gen_server:call(Name, {set_int_data, Type, Param})
    end.

single_read(Name, Key, Node) ->
    TxId = tx_utilities:create_tx_id(0),
    read(Name, TxId, Key, Node).

read(Name, TxId, Key, Node) ->
    case is_integer(Name) of
        true ->
            gen_server:call(generate_module_name((Name-1) rem ?NUM_SUP + 1), {read, Key, TxId, Node}, ?READ_TIMEOUT);
        false ->
            gen_server:call(Name, {read, Key, TxId, Node}, ?READ_TIMEOUT)
            %gen_server:call({global, Name}, {read, Key, TxId, Node}, ?READ_TIMEOUT)
    end.

clean_all_data() ->
    Parts = hash_fun:get_partitions(),
    Set = lists:foldl(fun({_, N}, D) ->
                sets:add_element(N, D)
                end, sets:new(), Parts),
    AllNodes = sets:to_list(Set),
    MySelf = self(),
    lager:info("Sending msg to ~w", [AllNodes]),
    lists:foreach(fun(Node) ->
                    spawn(rpc, call, [Node, tx_cert_sup, clean_data, [MySelf]])
    end, AllNodes),
    lager:info("Send cleaning"),
    lists:foreach(fun(_) ->
                   receive cleaned -> ok end
                   end, AllNodes).

clean_data(Sender) ->
    SPL = lists:seq(1, ?NUM_SUP),
    MySelf = self(),

    lager:info("~w created", [node()]),
    lists:foreach(fun(N) -> gen_server:cast(generate_module_name(N), {clean_data, MySelf}) end, SPL),
    lists:foreach(fun(_) -> receive cleaned -> ok end  end, SPL),
    lager:info("Got reply from all tx servers"),
    DataRepls = repl_fsm_sup:generate_data_repl_serv(),
    lists:foreach(fun({N, _, _, _, _, _}) -> lager:info("Sending to ~w", [N]), data_repl_serv:clean_data(N,  MySelf)  end, DataRepls),
    lists:foreach(fun(_) ->  receive cleaned -> ok end  end, DataRepls),
    lager:info("Got reply from all data_repls"),
    S = hash_fun:get_local_servers(),
    lists:foreach(fun(N) ->  clocksi_vnode:clean_data(N,  MySelf)  end, S),
    lists:foreach(fun(_) ->  receive cleaned -> ok end  end, S),
    lager:info("Got reply from all local_nodes"),
    gen_server:call(node(), {clean_data}),
    Sender ! cleaned.

get_all_size() ->
    Parts = hash_fun:get_partitions(),
    Set = lists:foldl(fun({_, N}, D) ->
                sets:add_element(N, D)
                end, sets:new(), Parts),
    AllNodes = sets:to_list(Set),
    lager:warning("Send alreay"),
    lists:foreach(fun(Node) ->
                    spawn(rpc, call, [Node, tx_cert_sup, get_size, [self()]])
    end, AllNodes),
    lager:warning("Waiting"),
    R = lists:foldl(fun(_, {PS, CS, DS, AllS}) ->
                   receive {S1, S2, S3, S4} -> 
                    {PS+S1, CS+S2, DS+S3, AllS+S4} 
                   end end, {0,0,0,0}, AllNodes),
    lager:warning("Final R is ~w", [R]).

read_all_nodes() ->
    Parts = hash_fun:get_partitions(),
    Set = lists:foldl(fun({_, N}, D) ->
                sets:add_element(N, D)
                end, sets:new(), Parts),
    AllNodes = sets:to_list(Set),
    lager:warning("Send alreay"),
    lists:foreach(fun(Node) ->
                    spawn(rpc, call, [Node, tx_cert_sup, read_all, []])
    end, AllNodes).

read_all() ->
    ToReplicate = repl_fsm_sup:find_to_repl(),
    DataRepl = lists:foldl(fun(Node, Acc) ->
            ReplName = list_to_atom(atom_to_list(node())++"repl"++atom_to_list(Node)),
            [ReplName|Acc] end, [], ToReplicate),
    Parts = hash_fun:get_partitions(),
    MyParts = [{Part, Node} || {Part, Node} <- Parts, Node == node()],
    lists:foreach(fun(N) -> 
                    data_repl_serv:read_all(N)
                  end, DataRepl),
    lists:foreach(fun(N) -> 
                    clocksi_vnode:read_all(N)
                  end, MyParts).

get_size(Sender) ->
    lager:warning("Got request from ~w", [Sender]),
    ToReplicate = repl_fsm_sup:find_to_repl(),
    DataRepl = lists:foldl(fun(Node, Acc) ->
            ReplName = list_to_atom(atom_to_list(node())++"repl"++atom_to_list(Node)),
            [ReplName|Acc] end, [], ToReplicate),
    lager:warning("Got all data repl"),
    Parts = hash_fun:get_partitions(),
    lager:warning("Got all parts "),
    MyParts = [{Part, Node} || {Part, Node} <- Parts, Node == node()],
    lager:warning("My parts are ~w, data repl is ~w ", [MyParts, DataRepl]),
    {PS, CS, DS, AllS} = 
        lists:foldl(fun(N, {S1, S2, S3, S4}) -> 
                        lager:warning("Sending to ~w", [N]),
                        {DS1, DS2, DS3, DS4} = data_repl_serv:get_size(N),
                        lager:warning("Got reply from ~w", [N]),
                        {S1+DS1, S2+DS2, S3+DS3, S4+DS4}
                    end, {0, 0, 0, 0}, DataRepl),
    {PS1, CS1, DS1, AllS1} = 
        lists:foldl(fun(N, {S1, S2, S3, S4}) -> 
                        lager:warning("Sending to ~w", [N]),
                        {DS1, DS2, DS3, DS4} = clocksi_vnode:get_size(N),
                        lager:warning("Got reply from ~w", [N]),
                        {S1+DS1, S2+DS2, S3+DS3, S4+DS4}
                    end, {PS, CS, DS, AllS}, MyParts),
    lager:warning("PS is ~w, CS is ~w, DS is ~w, AllS is ~w", [PS1, CS1, DS1, AllS1]),
    Sender ! {PS1, CS1, DS1, AllS1}.

get_all_oldest() ->
    Parts = hash_fun:get_partitions(),
    Set = lists:foldl(fun({_, N}, D) ->
                sets:add_element(N, D)
                end, sets:new(), Parts),
    AllNodes = sets:to_list(Set),
    lists:foreach(fun(Node) ->
                    spawn(rpc, call, [Node, tx_cert_sup, get_oldest, [self()]])
    end, AllNodes),
    lists:foldl(fun(_, OldT) ->
                   receive T -> 
                        lager:info("Got reply of T", [T]),
                        case OldT of
                         nil -> T;
                          _ -> case T of nil -> OldT;
                                              _ ->
                                          case T#tx_id.snapshot_time < OldT#tx_id.snapshot_time of
                                              true -> T;
                                              false -> OldT
                                          end
                                end  
                        end
                   end end, nil, AllNodes).

get_oldest() ->
    get_oldest(self()).

get_oldest(Sender) ->
    lager:info("Received T"),
    SPL = lists:seq(1, ?NUM_SUP),
    R = lists:foldl(fun(N, OldT) ->
            case gen_server:call(generate_module_name(N), {get_oldest}) of 
                nil ->  OldT;
                T -> case OldT of
                        nil -> T;
                        _ -> case T of nil -> OldT; 
                                            _ ->
                                        case T#tx_id.snapshot_time < OldT#tx_id.snapshot_time of
                                            true -> T;
                                            false -> OldT
                                        end
                    end end 
            end end, nil, SPL),
    lager:info("~w has replied!", [self()]),
    Sender ! R.

get_stat() ->
    SPL = lists:seq(1, ?NUM_SUP),
    lists:foldl(fun(N, Acc) ->
            Res = gen_server:call(generate_module_name(N), {get_stat}),
            lager:info("Get stat from ~w is ~p", [N, Res]),
            %AllZeros = lists:duplicate(7, 0),
            add_two(Res, Acc, [])
            end, lists:duplicate(7,0), SPL).
    %LocalServ = hash_fun:get_local_servers(),
    %PRead = lists:foldl(fun(S, Acc) ->
    %                    Num = helper:num_specula_read(S), Num+Acc
    %                    end, 0, LocalServ), 
    %Lists = antidote_config:get(to_repl),
    %[LocalRepList] = [LocalReps || {Node, LocalReps} <- Lists, Node == node()],
    %LocalRepNames = [list_to_atom(atom_to_list(node())++"repl"++atom_to_list(L))  || L <- LocalRepList ],
    %{DSpeculaRead, DTotalRead} = lists:foldl(fun(S, {Acc1, Acc2}) ->
    %                    {Num1, Num2} = data_repl_serv:num_specula_read(S), {Num1+Acc1, Num2+Acc2}
    %                    end, {0, 0}, LocalRepNames), 
    %lager:info("Data replica specula read is ~w, Data replica read is ~w", [DSpeculaRead, DTotalRead]),
    %{CacheSpeculaRead, CacheAttemptRead} = cache_serv:num_specula_read(),
    %RealCnt = max(1, Cnt),
    %{ListA, ListB} = lists:split(7, OtherResult),
    %L1 = ListA ++ [PRead, DSpeculaRead, DTotalRead, CacheSpeculaRead, CacheAttemptRead],
    %Avg = lists:map(fun(E) -> E div RealCnt end, ListB),
    %ListA ++ Avg.    

generate_module_name(N) ->
    list_to_atom(atom_to_list(node()) ++ "-cert-" ++ integer_to_list((N-1) rem ?NUM_SUP + 1)).

generate_supervisor_spec(N) ->
    Module = generate_module_name(N),
    case ets:lookup(meta_info, do_specula) of
        [{do_specula, true}] ->
            {Module,
             {specula_tx_cert_server, start_link, [Module]},
              permanent, 5000, worker, [specula_tx_cert_server]};
        [{do_specula, false}] ->
            {Module,
             {i_tx_cert_server, start_link, [Module]},
              permanent, 5000, worker, [i_tx_cert_server]}
    end.

get_pid(WorkerId) ->
    case is_integer(WorkerId) of
        true -> whereis(generate_module_name(WorkerId));
        false -> whereis(WorkerId)
    end.

get_hitcounters() ->
    SPL = lists:seq(1, ?NUM_SUP),
    R = lists:foldl(fun(N, HT) ->
            case gen_server:call(generate_module_name(N), {get_hitcounter}) of 
                Num -> HT+Num 
            end end, 0, SPL),
    lager:warning("Total hitcounter is ~w", [R]),
    R.
    

get_pids(Names) ->
    AllPids = lists:foldl(fun(WorkerId, Acc) ->
                case is_integer(WorkerId) of
                    true -> [whereis(generate_module_name(WorkerId))|Acc];
                    false -> [whereis(WorkerId)|Acc]
                end
                end, [], Names),
    lists:reverse(AllPids).

get_global_pid(Name) ->
    gen_server:call({global, Name}, {get_pid}).

%% @doc Starts the coordinator of a ClockSI interactive transaction.
init([]) ->
    Pool = [generate_supervisor_spec(N) || N <- lists:seq(1, ?NUM_SUP)],
    {ok, {{one_for_one, 5, 10}, Pool}}.

add_two([], [], R) ->
    lists:reverse(R);
add_two([H1|R1], [H2|R2], R) ->
    add_two(R1, R2, [(H1+H2)|R]).
