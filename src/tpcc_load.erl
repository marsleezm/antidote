%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(tpcc_load).

-export([load/1, thread_load/6]).

-include("tpcc.hrl").

-define(TIMEOUT, 20000).

%% ====================================================================
%% API
%% ====================================================================

load(WPerDc) ->
    lager:info("Start tpcc load at ~w", [node()]),
    random:seed(now()), %now()),
    %_PbPorts = basho_bench_config:get(antidote_pb_port),
    %% Choose the node using our ID as a modulus

    {PartList, ReplList, _} =  hash_fun:get_hash_fun(), %gen_server:call({global, MyTxServer}, {get_hash_fun}),
    %lager:info("Part list is ~w, Replist is ~w", [PartList, ReplList]),
    AllDcs = [N || {N, _} <- PartList],
    MyNode = node(),
    [M] = [L || {N, L} <- ReplList, N == MyNode ],
    MyRepIds = get_indexes(M, AllDcs),
    FullPartList = lists:flatten([L || {_, L} <- PartList]),
    HashLength = length(FullPartList),
    MyReps = lists:map(fun(Index) ->  Name = lists:nth(Index, AllDcs),  {Index, {rep, get_rep_name(MyNode, Name)}} end, MyRepIds),
    DcId = index(MyNode, AllDcs),
    NumDcs = length(AllDcs),
    lager:info("TargetNode is ~p, DcId is ~w, My Replica Ids are ~w",[MyNode, DcId, MyReps]),
    StartTime = os:timestamp(),
    case DcId of 1 ->
        init_params(FullPartList, HashLength),
        lager:info("Finish putting.");
                _ -> timer:sleep(4000)
    end,
    try
        ets:delete(tpcc_load)
    catch
        error:badarg ->  lager:warning("Ets table already deleted!")
    end,
    ets:new(tpcc_load, [named_table, public, set]),
    Part1 = get_partition("COMMIT_TIME", FullPartList, HashLength),
    lager:warning("Part1 is ~w!!!", [Part1]),
    {ok, COMMIT_TIME} = clocksi_vnode:internal_read(Part1, "COMMIT_TIME", tx_utilities:create_tx_id(0)),
    %lager:info("Got commit, is ~w", [COMMIT_TIME]),
    ets:insert(tpcc_load, {"COMMIT_TIME", COMMIT_TIME}),
    
    ToPopulateParts = [{DcId, server}|MyReps],
    lists:foreach(fun({Id, Server}) -> 
        lager:info("Spawning for dc ~w", [Id]),
        Tables = case Server of server -> 
                                    {_, L} = lists:nth(DcId, PartList),
                                    lists:foldl(fun(P, Acc) ->  Tab = clocksi_vnode:get_table(P), Acc++[Tab] end, [], L);
                                {rep, S} -> data_repl_serv:get_table(S)
                end,
        spawn(tpcc_load, thread_load, [Id, Tables, WPerDc, PartList, NumDcs, self()]) end, ToPopulateParts),
    lists:foreach(fun(_) ->  receive {done, I, S} -> ok, lager:info("Receive a reply from ~w, ~w", [I, S]) end end, ToPopulateParts), 
    ets:delete(tpcc_load),
    EndTime = os:timestamp(),
    lager:info("Population finished.. used ~w", [timer:now_diff(EndTime, StartTime)/1000000]).

thread_load(DcId, Server, WPerDc, PartList, NumDcs, Sup) ->
    lager:info("Started in thread load! My tables are ~w", [Server]),
    RepWSeq = lists:seq(WPerDc*(DcId-1)+1, DcId*WPerDc),
    lager:info("Populating items for dc ~w", [DcId]),
    populate_items(Server, NumDcs, DcId, PartList),

    lager:info("Populating warehouse for dc ~w", [DcId]),
    lists:foreach(fun(RepWId) -> populate_warehouse(Server, RepWId, PartList, WPerDc) end, RepWSeq), 

    lager:info("Populating stocks for dc ~w", [DcId]),
    lists:foreach(fun(RepWId) -> populate_stock(Server, RepWId, PartList, WPerDc) end, RepWSeq), 
    DistSeq = lists:seq(1, ?NB_MAX_DISTRICT),
    lists:foreach(fun(DistrictId) ->
        lager:info("Populating districts ~w for dc ~w", [DistrictId, DcId]),
        lists:foreach(fun(RepWId) -> populate_district(Server, RepWId, DistrictId, PartList, WPerDc) end, RepWSeq), 
        lager:info("Populating customer orders for ~w", [DistrictId]),
        lists:foreach(fun(RepWId) -> populate_customer_orders(Server, RepWId, DistrictId, PartList, WPerDc) end, RepWSeq)
        end,  DistSeq),
    Sup ! {done, DcId, Server},
    ok.

populate_items(TxServer, NumDCs, DcId, PartList) ->
    random:seed({DcId, 122,32}),
    Remainder = ?NB_MAX_ITEM rem NumDCs,
    DivItems = (?NB_MAX_ITEM - Remainder)/NumDCs,
    FirstItem = ((DcId-1) * DivItems) + 1,
    LastItem = case DcId of
                    NumDCs ->
                        DivItems + Remainder + FirstItem - 1;
                    _ ->
                        DivItems + FirstItem -1
                   end,
    put_range_items(TxServer, trunc(FirstItem), trunc(LastItem), DcId, PartList).
    %put_range_items(TxServer, 1, ?NB_MAX_ITEM, DcId, PartList).

populate_warehouse(TxServer, WarehouseId, PartList, WPerDc)->
    random:seed({WarehouseId, 12,32}),
    Warehouse = tpcc_tool:create_warehouse(WarehouseId),
    WKey = tpcc_tool:get_key(Warehouse),
    WYtd = ?WAREHOUSE_YTD,
    WYtdKey = WKey++":w_ytd",
    lager:info("Populating ~p", [WKey]),
    lager:info("Populating ~p", [WYtdKey]),
    put_to_node(TxServer, to_dc(WarehouseId, WPerDc), PartList, WYtdKey, WYtd),
    put_to_node(TxServer, to_dc(WarehouseId, WPerDc), PartList, WKey, Warehouse).

populate_stock(TxServer, WarehouseId, PartList, WPerDc) ->
    random:seed({WarehouseId, 2,222}),
    Seq = lists:seq(1, ?NB_MAX_ITEM),
    lager:info("Warehouse ~w: Populating stocks from 1 to ~w", [WarehouseId, ?NB_MAX_ITEM]),
    DcId = to_dc(WarehouseId, WPerDc),
    
    lists:foreach(fun(StockId) ->
                      Stock = tpcc_tool:create_stock(StockId, WarehouseId),
                      Key = tpcc_tool:get_key(Stock),
                      put_to_node(TxServer, DcId, PartList, Key, Stock)
                      %defer_put(DcId, PartList, Key, Stock, WriteSet)
                      end, Seq).

populate_district(TxServer, WarehouseId, DistrictId, PartList, WPerDc) ->
    random:seed({WarehouseId, DistrictId, 22}),
    lager:info("Warehouse ~w: Populating district ~w", [WarehouseId, DistrictId]),
    DcId = to_dc(WarehouseId, WPerDc),
    District = tpcc_tool:create_district(DistrictId, WarehouseId),
    DKey = tpcc_tool:get_key(District),
    put_to_node(TxServer, DcId, PartList, DKey, District),
    DYtd = ?WAREHOUSE_YTD,
    YtdKey = DKey++":d_ytd",
    put_to_node(TxServer, DcId, PartList, YtdKey, DYtd).

populate_customer_orders(TxServer, WarehouseId, DistrictId, PartList, WPerDc) ->
    lager:info("Warehouse ~w, district ~w: Populating orders from 1 to ~w", [WarehouseId, DistrictId, 
                ?NB_MAX_ORDER]),
    NumUniqueNames = ?NUM_NAMES *?NUM_NAMES*?NUM_NAMES, 
    FCustomerSeq = lists:seq(1, NumUniqueNames),
    SCustomerSeq = lists:seq(NumUniqueNames+1, ?NB_MAX_CUSTOMER),
    %% Randomize the list of all combination of names
    FNameRandSeq = [X||{_,X} <- lists:sort([ {random:uniform(), N} || N <- FCustomerSeq])],
    SNameRandSeq = [tpcc_tool:c_last_rand(WarehouseId) || _N <- SCustomerSeq],
    random:seed({WarehouseId, DistrictId, 1111}),
    %% The list of customer
    OrderSeq = lists:seq(1, ?NB_MAX_ORDER),
    RandOrders = [X||{_,X} <- lists:sort([ {random:uniform(), N} || N <- OrderSeq])],
    {FRandOrders, SRandOrders} = lists:split(NumUniqueNames, RandOrders),
    [{"COMMIT_TIME", CommitTime}] = ets:lookup(tpcc_load, "COMMIT_TIME"),
    %% Magic number, assume that the order is created 1 sec ago.
    DcId = to_dc(WarehouseId, WPerDc),
    add_customer_order(1, FNameRandSeq, FRandOrders, TxServer, WPerDc, DcId, PartList, WarehouseId, DistrictId, CommitTime, defer),
    add_customer_order(NumUniqueNames+1, SNameRandSeq, SRandOrders, TxServer, WPerDc, DcId, PartList, WarehouseId, DistrictId, CommitTime, direct).

add_customer_order(_, [], [], _, _, _, _, _, _, _, _) ->
    ok;
add_customer_order(CustomerId, [NameNum|RestName], [RandOrder|RestOrder], TxServer, WPerDc, DcId, PartList, WarehouseId, DistrictId, CommitTime, Type) ->
    Date = CommitTime - 1000,
    HistoryTime = CommitTime - 123,
    CustomerTime = CommitTime - 13,

    CLast = tpcc_tool:name_by_num(NameNum),
    OrderId = RandOrder, 
    %lager:info("Customer id is ~w, order is ~w, clast is ~p", [CustomerId, RandOrder, CLast]),
    Customer = tpcc_tool:create_customer(WarehouseId, DistrictId, CustomerId, CLast, CustomerTime, OrderId),
    CKey = tpcc_tool:get_key(Customer),
    put_to_node(TxServer, DcId, PartList, CKey, Customer),
    CBalanceKey = CKey++":c_balance",
    put_to_node(TxServer, DcId, PartList, CBalanceKey, -10),
    CLKey = tpcc_tool:get_key_by_param({WarehouseId, DistrictId, CLast}, customer_lookup),
    CustomerLookup = case Type of
                        direct -> read_from_node(TxServer, CLKey, DcId, PartList);
                        defer -> tpcc_tool:create_customer_lookup(WarehouseId, DistrictId, CLast)
                     end,
    Ids = CustomerLookup#customer_lookup.ids,
    CustomerLookup1 = CustomerLookup#customer_lookup{ids=[CustomerId|Ids]}, 
    put_to_node(TxServer, to_dc(WarehouseId, WPerDc), PartList, CLKey, CustomerLookup1),
    History = tpcc_tool:create_history(WarehouseId, DistrictId, CustomerId, HistoryTime),
    HKey = tpcc_tool:get_key(History),
    put_to_node(TxServer, DcId, PartList, HKey, History),

    %Date = tpcc_tool:now_nsec(),
    OOlCnt = tpcc_tool:random_num(5, 15),
    Order = tpcc_tool:create_order(WarehouseId, DistrictId, OrderId, OOlCnt, CustomerId,
        Date),
    OKey = tpcc_tool:get_key(Order),
    put_to_node(TxServer, to_dc(WarehouseId, WPerDc), PartList, OKey, Order),
    populate_orderlines(TxServer, WarehouseId, DistrictId, OrderId, OOlCnt, Date, PartList, WPerDc),
    case OrderId >= ?LIMIT_ORDER of
        true ->
            NewOrder = tpcc_tool:create_neworder(WarehouseId, DistrictId, OrderId),
            NOKey = tpcc_tool:get_key(NewOrder),
            put_to_node(TxServer, DcId, PartList, NOKey, NewOrder),
            add_customer_order(CustomerId+1, RestName, RestOrder, TxServer, WPerDc, DcId, PartList, WarehouseId, DistrictId, CommitTime, Type);
        false ->
            add_customer_order(CustomerId+1, RestName, RestOrder, TxServer, WPerDc, DcId, PartList, WarehouseId, DistrictId, CommitTime, Type)
    end.

populate_orderlines(TxServer, WarehouseId, DistrictId, OrderId, OOlCnt, Date, PartList, WPerDc) ->
    %lager:info("Warehouse ~w, district ~w: Populating orderlines from 1 to ~w", [WarehouseId, DistrictId, 
    %            OOlCnt]),
    Seq = lists:seq(1, OOlCnt),
    DcId = to_dc(WarehouseId, WPerDc),
    lists:foreach(fun(OrderlineId) -> 
                    random:seed({WarehouseId, DistrictId, OrderlineId}),
                    {Amount, DDate} = case OrderId >= ?LIMIT_ORDER of
                                        true ->
                                            {tpcc_tool:random_float(0.01, 9999.99, 2), 0};
                                        false ->
                                            {-1, Date}
                                      end,
                    random:seed({WarehouseId, DistrictId, OrderlineId+10}),
                    OrderLine = tpcc_tool:create_orderline(WarehouseId, DistrictId, OrderId, OrderlineId, DDate, Amount),
                    OLKey = tpcc_tool:get_key(OrderLine),
                    put_to_node(TxServer, DcId, PartList, OLKey, OrderLine)
                end, Seq).
    
put_range_items(TxServer, FirstItem, LastItem, DcId, PartList) ->
    lager:info("Populating items from ~w to ~w", [FirstItem, LastItem]),
    Seq = lists:seq(FirstItem, LastItem),
    lists:foreach(fun(ItemId) ->
                    Item = tpcc_tool:create_item(ItemId),
                    Key = tpcc_tool:get_key(Item),
                    put_to_node(TxServer, DcId, PartList, Key, Item)
                    end, Seq).

init_params(FullPartList, HashLength) ->
    K1 = "C_C_LAST",
    K2 = "C_C_ID",
    K3 = "C_OL_I_ID",
    K4 = "COMMIT_TIME", 
    C_C_LAST = tpcc_tool:random_num(0, ?A_C_LAST),
    C_C_ID = tpcc_tool:random_num(0, ?A_C_ID),
    C_OL_I_ID = tpcc_tool:random_num(0, ?A_OL_I_ID),
    COMMIT_TIME = tpcc_tool:now_nsec() - 100000,
    Partition1 = get_partition(K1, FullPartList, HashLength),
    Partition2 = get_partition(K2, FullPartList, HashLength),
    Partition3 = get_partition(K3, FullPartList, HashLength),
    Partition4 = get_partition(K4, FullPartList, HashLength),
    lager:info("Putting CCLAST to ~w", [Partition1]),
    lager:info("Putting CCID to ~w", [Partition2]),
    lager:info("Putting COLIID to ~w", [Partition3]),
    lager:info("Putting COMMIT_TIME to ~w", [Partition4]),
    {ok, _} = clocksi_vnode:append_values(Partition1, [{K1, C_C_LAST}], COMMIT_TIME, self()),
    %lager:info("Finish putting CCLAST to ~w", [Partition1]),
    clocksi_vnode:append_values(Partition2, [{K2, C_C_ID}], COMMIT_TIME, self()),
    %lager:info("Finish putting CCID to ~w", [Partition2]),
    clocksi_vnode:append_values(Partition3, [{K3, C_OL_I_ID}], COMMIT_TIME, self()),
    %lager:info("Finish putting COLIID to ~w", [Partition3]),
    clocksi_vnode:append_values(Partition4, [{K4, COMMIT_TIME}], COMMIT_TIME, self()).
    %lager:info("Finish putting COMMIT_TIME to ~w", [Partition4]).

get_partition(Key, PartList, HashLength) ->
    Num = crypto:bytes_to_integer(erlang:md5(Key)) rem HashLength +1,
    lists:nth(Num, PartList).
    
put_to_node(Tabs, _DcId, _PartList, Key, Value) ->
    [{"COMMIT_TIME", CommitTime}] = ets:lookup(tpcc_load, "COMMIT_TIME"),
    case is_list(Tabs) of
        true -> %% Is clocksi table
            Index = crypto:bytes_to_integer(erlang:md5(Key)) rem length(Tabs) + 1,
            Tab = lists:nth(Index, Tabs),
            %lager:info("Putting ~p ~p to ~w", [Key, Value, Tab]),
            ets:insert(Tab, {Key, [{CommitTime, Value}]});
        false ->
            ets:insert(Tabs, {Key, [{CommitTime, Value}]})
    end.

%defer_put(DcId, PartList, Key, Value, WriteSet) ->
%    {_, L} = lists:nth(DcId, PartList),
%    Index = crypto:bytes_to_integer(erlang:md5(Key)) rem length(L) + 1,
%    dict:append(Index, {Key, Value}, WriteSet).

%multi_put(TxServer, DcId, PartList, WriteSet) ->
%    {_, L} = lists:nth(DcId, PartList%),
%    DictList = dict:to_list(WriteSet),
%    [{"COMMIT_TIME", CommitTime}] = ets:lookup(tpcc_load, "COMMIT_TIME"),
%    lists:foreach(fun({Index, KeyValues}) ->
%            Part = lists:nth(Index, L),    
%            put(TxServer, Part, KeyValues, CommitTime)
%            end, DictList).              

%put(TxServer, Part, KeyValues, CommitTime) ->
%    %lager:info("Single Puting [~p, ~p] to ~w", [Key, Value, Part]),
%    case TxServer of
%        server ->
%            {ok, _} = clocksi_vnode:append_values(Part, KeyValues, CommitTime, self());
%        {rep, S} ->
%            ok = data_repl_serv:append_values(S, KeyValues, CommitTime)
%    end.

read_from_node(Tabs, Key, _DcId, _PartList) ->
    case is_list(Tabs) of
          true -> %% Is clocksi table
              Index = crypto:bytes_to_integer(erlang:md5(Key)) rem length(Tabs) + 1,
              Tab = lists:nth(Index, Tabs),
              [{Key, [{_, Value}]}] = ets:lookup(Tab, Key),
              Value;
          false ->
              [{Key, [{_, Value}]}] = ets:lookup(Tabs, Key),
              Value
    end.

index(Elem, L) ->
    index(Elem, L, 1).

index(_, [], _) ->
    -1;
index(E, [E|_], N) ->
    N;
index(E, [_|L], N) ->
    index(E, L, N+1).

get_indexes(PL, List) ->
    %lager:info("Trying to get index: PL ~w, List ~w", [PL, List]),
    [index(X, List) || X <- PL ].

get_rep_name(Target, Rep) ->
    list_to_atom(atom_to_list(Target)++"repl"++atom_to_list(Rep)).

to_dc(WId, WPerDc) ->
    (WId-1) div WPerDc + 1.
