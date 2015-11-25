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
-module(antidote_pb_txn).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("antidote.hrl").
-include_lib("riak_pb/include/antidote_pb.hrl").


-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3,
         random_string/1
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #fpbtxnreq{} ->
            {ok, Msg, {"antidote.generaltxn",<<>>}};
        #fpbpartlistreq{} ->
            {ok, Msg, {"antidote.partlistreq",<<>>}};
        #fpbsingleupreq{} ->
            {ok, Msg, {"antidote.singleupreq",<<>>}};
        #fpbstarttxnreq{} ->
            {ok, Msg, {"antidote.starttxn",<<>>}};
        #fpbpreptxnreq{} ->
            {ok, Msg, {"antidote.preptxn",<<>>}};
        #fpbreadreq{} ->
            {ok, Msg, {"antidote.readreq",<<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbtxnreq{ops = Ops}, State) ->
    Updates = decode_general_txn(Ops),
    case antidote:execute_g_tx([Updates]) of
        {error, _Reason} ->
            {reply, #fpbtxnresp{success = false}, State};
        {ok, {_Txid, ReadSet, CommitTime}} ->
            ReadReqs = lists:filter(fun(Op) -> case Op of 
                            {update, _, _, _} -> false; {read, _, _} -> true end end, Updates),
            Zipped = lists:zip(ReadReqs, ReadSet), 
            Reply = encode_general_txn_response(Zipped),
            {reply, #fpbtxnresp{success=true,
                                            clock= term_to_binary(CommitTime),
                                            results=Reply}, State}
    end;
process(#fpbstarttxnreq{clock=Clock}, State) ->
    %lager:info("Start txn req"),
    TxId = tx_utilities:create_tx_id(Clock),
    {reply, #fpbtxid{snapshot=TxId#tx_id.snapshot_time, pid=term_to_binary(TxId#tx_id.server_pid)}, State};
process(#fpbpreptxnreq{txid=TxId, threadid=ThreadId, 
            local_updates=LocalUpdates, remote_updates=RemoteUpdates}, State) ->
    %lager:info("Prep txn req"),
    %T1 = tx_utilities:now_microsec(),
    RealId= decode_txid(TxId),
    DeLocalUpdates = decode_update_list(LocalUpdates),
    DeRemoteUpdates = decode_update_list(RemoteUpdates),
    %T2 = tx_utilities:now_microsec(),
    %lager:info("Decode takes ~w", [T2-T1]),
    case antidote:prepare(ThreadId, RealId, DeLocalUpdates, DeRemoteUpdates) of
        {ok, {committed, CommitTime}} ->
            %T3 = tx_utilities:now_microsec(),
            %lager:info("Commit takes ~w", [T3-T2]),
            {reply, #fpbpreptxnresp{result=2, commit_time=CommitTime}, State};
        {ok, {specula_commit, CommitTime}} ->
            {reply, #fpbpreptxnresp{result=1, commit_time=CommitTime}, State};
        {aborted, RealId} ->
            %T3 = tx_utilities:now_microsec(),
            %lager:warning("~w: aborted, takes ~w!", [RealId, T3-T2]),
            {reply, #fpbpreptxnresp{result=0}, State};
        Reason ->
            lager:warning("Error reason: ~w", [Reason]),
            {reply, #fpbpreptxnresp{result=false}, State}
    end;
process(#fpbreadreq{txid=TxId, threadid=ThreadId, key=Key, replica_ip=ReplicaIp, 
            node_id=NodeId, partition_id=PartitionId}, State) ->
    %lager:info("Trying to read, TxId is ~s, key is ~t", [TxId, binary_to_list(Key)]),
    %T1 = tx_utilities:now_microsec(),
    RealTxId = case TxId of
                    undefined ->
                        tx_utilities:create_tx_id(0);
                    _ ->
                        decode_txid(TxId)
                end,
    %T2 = tx_utilities:now_microsec(),
    %lager:info("Decode tx id takes ~w", [T2-T1]),
    Node = case ReplicaIp of
                undefined ->
                    hash_fun:get_vnode_by_id(PartitionId, NodeId);
                _ ->
                    list_to_atom(atom_to_list(node())++"repl"++ReplicaIp)
            end,
    {ok, Value} = case ThreadId of
                        undefined ->
                            tx_cert_sup:read(random:uniform(4), RealTxId, Key, Node);
                        _ ->
                             tx_cert_sup:read(ThreadId, RealTxId, Key, Node)
    end,
    case Value of
        [] ->
            %T3 = tx_utilities:now_microsec(),
            %lager:info("read takes ~w", [T3-T2]),
            %{reply, #fpbvalue{field=12, str_value=random_string(500)}, State};
            {reply, #fpbvalue{field=2}, State};
        _ ->
            %EV = encode_value(Value),
            %T3 = tx_utilities:now_microsec(),
            %lager:info("read and encode takes ~w", [T3-T2]),
            {reply, encode_value(Value), State}
    end;
process(#fpbsingleupreq{key=Key, value=Value, partition_id=PartitionId}, State) ->
    %lager:info("Key is ~s, value is ~w, partition id is ~w", [Key, Value, PartitionId]),
    Result = antidote:single_commit(hash_fun:get_local_vnode_by_id(PartitionId), 
                Key, decode_value(Value)),
    case Result of
        {ok, {committed, CommitTime}} ->
            {reply, #fpbpreptxnresp{result=1, commit_time=CommitTime}, State};
        {aborted, RealId} ->
            lager:warning("~w: aborted!", [RealId]),
            {reply, #fpbpreptxnresp{result=0}, State};
        Reason ->
            lager:warning("Error reason: ~w", [Reason]),
            {reply, #fpbpreptxnresp{result=0}, State}
    end;
process(#fpbpartlistreq{noop=_}, State) ->
    {PartList, ReplList} = hash_fun:get_hash_fun(),
    lager:info("Hash fun is ~w", [PartList]),
    {reply, #fpbpartlist{node_parts=encode_part_list(PartList), repl_list=encode_repl_list(ReplList)}, State}.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

decode_general_txn(Ops) ->
    lists:map(fun(Op) -> decode_general_txn_op(Op) end, Ops). 

decode_update_list(#fpbnodeups{per_nodeup=Ops}) ->
    lists:map(fun(Op) -> decode_node_updates(Op) end, Ops).

decode_node_updates(#fpbpernodeup{node_id=NodeId, partition_id=PartitionId, ups=Updates}) ->
    {hash_fun:get_vnode_by_id(PartitionId, NodeId), 
        lists:map(fun(Up) ->  decode_update(Up) end, Updates)}.

decode_update(#fpbupdate{key=Key, value=Value}) ->
    %lager:info("Key is ~t", [Key]),
    {Key, decode_value(Value)}.

decode_value(#fpbvalue{field=2, customer=Value}) ->
    {2, Value}; 
decode_value(#fpbvalue{field=3, clookup=Value}) ->
    {3, Value}; 
decode_value(#fpbvalue{field=4, district=Value}) ->
    {4, Value}; 
decode_value(#fpbvalue{field=5, history=Value}) ->
    {5, Value}; 
decode_value(#fpbvalue{field=6, item=Value}) ->
    {6, Value}; 
decode_value(#fpbvalue{field=7, neworder=Value}) ->
    {7, Value}; 
decode_value(#fpbvalue{field=8, order=Value}) ->
    {8, Value}; 
decode_value(#fpbvalue{field=9, orderline=Value}) ->
    {9, Value}; 
decode_value(#fpbvalue{field=10, stock=Value}) ->
    {10, Value}; 
decode_value(#fpbvalue{field=11, warehouse=Value}) ->
    {11, Value}; 
decode_value(#fpbvalue{field=12, str_value=Value}) ->
    {12, Value}; 
decode_value(#fpbvalue{field=13, long_value=Value}) ->
    {13, Value}; 
decode_value(#fpbvalue{field=14, double_value=Value}) ->
    {14, Value}.


decode_general_txn_op(#fpbtxnop{type=0, key=Key, operation=Op, parameter=Param}) ->
    {update, Key, get_type_by_id(Op), {{get_op_by_id(Op), binary_to_term(Param)}, haha}};
decode_general_txn_op(#fpbtxnop{type=1, key=Key}) ->
    {read, Key}.

decode_txid(#fpbtxid{snapshot=Snapshot, pid=Pid}) ->
    #tx_id{snapshot_time=Snapshot, server_pid=binary_to_term(Pid)}.

encode_general_txn_response(Zipped) ->
    lists:map(fun(Resp) ->
                      encode_general_txn_read_resp(Resp)
              end, Zipped).

encode_general_txn_read_resp({{read, _Key, _Type}, Result}) ->
    #fpbvalue{str_value=term_to_binary(Result)}.

encode_part_list(List) ->
    lists:map(fun({Ip, NumPartitions}) -> 
         #fpbnodepart{ip=atom_to_list(Ip), num_partitions=NumPartitions} end , List).

encode_repl_list(List) ->
    lists:map(fun({Ip, ListIps}) -> 
         #fpbrepllist{ip=atom_to_list(Ip), to_repls=[atom_to_list(Node) || Node <- ListIps]} end, List).

encode_value({2, Value}) ->
    #fpbvalue{field=2, customer=Value};
encode_value({3, Value}) ->
    #fpbvalue{field=3, clookup=Value}; 
encode_value({4, Value}) ->
    #fpbvalue{field=4, district=Value}; 
encode_value({5, Value}) ->
    #fpbvalue{field=5, history=Value}; 
encode_value({6, Value}) ->
    #fpbvalue{field=6, item=Value}; 
encode_value({7, Value}) ->
    #fpbvalue{field=7, neworder=Value}; 
encode_value({8, Value}) ->
    #fpbvalue{field=8, order=Value}; 
encode_value({9, Value}) ->
    #fpbvalue{field=9, orderline=Value}; 
encode_value({10, Value}) ->
    #fpbvalue{field=10, stock=Value}; 
encode_value({11, Value}) ->
    #fpbvalue{field=11, warehouse=Value}; 
encode_value({12, Value}) ->
    #fpbvalue{field=12, str_value=Value}; 
encode_value({13, Value}) ->
    #fpbvalue{field=13, long_value=Value}; 
encode_value({14, Value}) ->
    #fpbvalue{field=14, double_value=Value};
encode_value(Value) ->
    #fpbvalue{field=12, str_value=Value}.

get_op_by_id(0) ->
    increment;
get_op_by_id(1) ->
    decrement;
get_op_by_id(2) ->
    assign;
get_op_by_id(3) ->
    add;
get_op_by_id(4) ->
    remove.

get_type_by_id(0) ->
    riak_dt_pncounter;
get_type_by_id(1) ->
    riak_dt_pncounter;
get_type_by_id(2) ->
    riak_dt_lwwreg;
get_type_by_id(3) ->
    riak_dt_orset;
get_type_by_id(4) ->
    riak_dt_orset.

random_string(Len) ->
    Chrs = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    ChrsSize = size(Chrs),
    F = fun(_, R) -> [element(random:uniform(ChrsSize), Chrs) | R] end,
    lists:foldl(F, "", lists:seq(1, Len)).
