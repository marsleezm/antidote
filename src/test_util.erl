-module(test_util).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([delete_1/2, delete_2/2, check_length1/2, check_time/1, check_length2/2,
         lookup_1/2, lookup_2/2, get_my_range/4, dict2/2,
         check_node/1, check_list/1, test_hash/2, ets2/2,
         bs/1, dict_store/3, dict_delete/2,
         ets_store/3, ets_delete/2, ets_das/1,
         pass1/1, pass2/1, set1/2, set2/2, set3/2]).

check_time(N) ->
    S = lists:seq(1, N),
    Start = now(),
    lists:foreach(fun(_) -> os:timestamp() end, S), 
    End = now(),
    io:format("Diff is ~w ~n", [timer:now_diff(End, Start)]).

check_length1(N, S) ->
    Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            case length(Seq) > 30 of
                true -> ok;  false -> ok
            end
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

check_length2(N, S) ->
    _Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            case S > 30 of
                true -> ok;  false -> ok
            end
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

set1(N, S) ->
    Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            lists:foldl(fun(M,Set) -> sets:add_element(M,Set) end, sets:new(), Seq)
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

set2(N, S) ->
    Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    Start = os:timestamp(),
    Set1 = lists:foldl(fun(M,Set) -> sets:add_element(M,Set) end, sets:new(), Seq),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            sets:is_element(10, Set1)
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Creation is ~w, Diff is ~w ~n", [timer:now_diff(T, Start),  Diff]).

dict2(N, S) ->
    Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    Start = os:timestamp(),
    Set1 = lists:foldl(fun(M,Set) -> dict:store(M, h, Set) end, dict:new(), Seq),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            dict:find(10, Set1)
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Creation is ~w, Diff is ~w ~n", [timer:now_diff(T, Start), Diff]).

ets2(N, S) ->
    ETS = ets:new(haha, [set]),
    Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    Start = os:timestamp(),
    lists:foreach(fun(M) -> ets:insert(ETS, {M, h}) end, Seq),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            ets:lookup(ETS, haha)
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Creation is ~w, Diff is ~w ~n", [timer:now_diff(T, Start), Diff]).

set3(N, S) ->
    Seq = lists:seq(1, S),
    SeqN = lists:seq(1, N),
    T = os:timestamp(),
    Set1 = lists:foldl(fun(M,Set) -> sets:add_element(M,Set) end, sets:new(), Seq),
    lists:foreach(fun(_) ->
            lists:foldl(fun(M,Se) -> sets:del_element(M,Se) end, Set1, Seq)
            end, SeqN),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

dict_store(S, Time, Lookup) ->
    Times = lists:seq(1, Time),
    T = os:timestamp(),
    lists:foreach(fun(_) -> 
    Seq = lists:seq(1, S),
    Dict1 = lists:foldl(fun(M,Set) -> dict:store(M, finished, Set) end, dict:new(), Seq),
    L = lists:seq(1, Lookup),
    lists:foreach(fun(_) -> dict:find(1, Dict1) end, L)
    end, Times),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w sec ~n", [Diff / 1000000]).

dict_delete(S, R) ->
    Seq = lists:seq(1, S),
    SeqR = lists:seq(1, R),
    T = os:timestamp(),
    Set1 = lists:foldl(fun(M,Set) -> dict:store(M, finished, Set) end, dict:new(), Seq),
    lists:foreach(fun(_) ->
            lists:foldl(fun(M,Set) -> dict:erase(M, Set)  end, Set1, Seq)
            end, SeqR),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

ets_store(S, Time, Lookup) ->
    Times = lists:seq(1, Time),
    T = os:timestamp(),
    lists:foreach(fun(_) -> 
    Table = ets:new(haha, [set]),
    Seq = lists:seq(1, S),
    _Dict1 = lists:foldl(fun(M,_Set) -> ets:insert(Table, {M, finished}) end, dict:new(), Seq),
    L = lists:seq(1, Lookup),
    lists:foreach(fun(_) -> ets:lookup(Table, 1) end, L), 
    ets:delete(Table)
    end, Times), 
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w sec ~n", [Diff / 1000000]).

ets_das(S) ->
    Table = ets:new(haha, [set]),
    Seq = lists:seq(1, S),
    T = os:timestamp(),
    _Dict1 = lists:foldl(fun(M,_Set) -> ets:insert(Table, {M, finished}) end, dict:new(), Seq),
    Diff = timer:now_diff(os:timestamp(), T),
    ets:delete(Table),
    io:format("Diff is ~w ~n", [Diff]).

ets_delete(S, R) ->
    Table = ets:new(haha, [set]),
    Seq = lists:seq(1, S),
    SeqR = lists:seq(1, R),
    T = os:timestamp(),
    Set1 = lists:foldl(fun(M,_Set) -> ets:insert(Table, {M, finished}) end, dict:new(), Seq),
    lists:foreach(fun(_) ->
            lists:foldl(fun(M,_Set) -> ets:delete(Table, M)  end, Set1, Seq)
            end, SeqR),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

test_hash(N, StrLen) ->
    Key = random_string(StrLen),
    PartList = [[100, 111,333], [444, 22333], [222, 3333], [1122, 1111]],
    Seq = lists:seq(1, N),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            L = lists:nth(2, PartList),
            crypto:bytes_to_integer(erlang:md5(Key)) rem length(L) + 1
            end, Seq),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

get_my_range(Total, Start, NumIds, MyId) ->
    Remainder = Total rem NumIds,
    Div = (Total-Remainder)/NumIds,
    First = ((MyId-1) * Div) + Start,
    Last = case MyId of
                NumIds ->
                  Div + Remainder + First - 1;
                _ ->
                  Div + First -1
               end,
    {First, Last}.

check_node(Times) ->
    Seq = lists:seq(1, Times),
    P = self(),
    A = now_nsec(),
    lists:map(fun(_) ->
           node(P) == node()
            end, Seq),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).
    
check_list(Times) ->
    Seq = lists:seq(1, Times),
    P = self(),
    A = now_nsec(),
    lists:map(fun(_) ->
        L = pid_to_list(P),
        lists:nth(2, L) == 48 
            end, Seq),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).


delete_1(Len, Elem) ->
    Seq = lists:seq(1, Len),
    L = [ {X, X*5, X*6}  || X <- Seq],
    A = now_nsec(),
    _Result = df1(Elem, L),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).
    %io:format("Used ~w, List is ~p", [B-A, Result]).

df1(Elem, [{_, _, Elem}|Rest]) ->
    Rest;
df1(Elem, [{A, B, E}|Rest]) ->
    [{A, B, E}|df1(Elem, Rest)].

delete_2(Len, Elem) ->
    Seq = lists:seq(1, Len),
    L = [ {X, X*5, X*6}  || X <- Seq],
    A = now_nsec(),
    _Result = df2(Elem, L, []),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).

df2(Elem, [{_, _, Elem}|Rest], Start) ->
    lists:reverse(Start)++Rest;
df2(Elem, [{A, B, E}|Rest], Start) ->
    df2(Elem, Rest, [{A,B,E}|Start]).

now_nsec() ->
    {A, B, C} = now(),
    (A * 1000000 + B) * 1000000 + C. 

lookup_1(Times, NumItems) ->
    Seq2 = lists:seq(1, Times),
    ets:new(table, [public, set, named_table]),
    Dict = init(NumItems),
    RandSeq = [random:uniform(10) || _ <- Seq2] ,  
    A = now_nsec(),
    lists:foldl(fun(X, D) ->
             check1(X, 8, D)
             end, Dict, RandSeq),
            B = now_nsec(),
    io:format("Used ~w ~n", [B-A]),
    ets:delete(table).

lookup_2(Times, NumItems) ->
    Seq2 = lists:seq(1, Times),
    ets:new(table, [public, set, named_table]),
    _ = init(NumItems),
    RandSeq = [random:uniform(10) || _ <- Seq2] ,  
    A = now_nsec(),
    lists:foreach(fun(X) ->
             check2(X, 8)
             end, RandSeq),
    B = now_nsec(),
    io:format("Used ~w ~n", [B-A]),
    ets:delete(table).

pass1(N) ->
    Seq = lists:seq(1, N),
    A = now_nsec(),
    lists:foreach(fun(_) ->
          fun1(1,2,3,4,5,6,7,8,9,10) end, Seq),
    B = now_nsec(),
    io:format("Used ~w ~n", [B-A]).

pass2(N) ->
    ets:new(table, [public, set, named_table]),
    ets:insert(table, {2,3,4,5,6,7,8,9}),
    Seq = lists:seq(1, N),
    A = now_nsec(),
    lists:foreach(fun(_) ->
          fun2(1) end, Seq),
    B = now_nsec(),
    io:format("Used ~w ~n", [B-A]),
    ets:delete(table).

fun1(_,_,_,_,_,_,_,_,_,_) ->
    ok.

fun2(_) ->
    ets:lookup(table,2),
    ok.

bs(_) ->
    Sml = 0,
    Big = 8,
    Dict = dict:new(),
    NewDict = lists:foldl(fun(N, D) ->
                dict:store(N, inf, D)
                end, Dict, lists:seq(Sml, Big)),
    T1 = 300,
    T2 = 200,
    T3 = 400,
    T4 = 200,
    T5 = 300,
    D1 = dict:store(Sml, T1, NewDict),
    {S1, B1, M1} = int_bs(D1, Sml, Big, Sml, inf),
    io:format("~w, ~w, ~w ~n", [S1, B1, M1]),
    D2 = dict:store(M1, T2, D1),
    {S2, B2, M2} = int_bs(D2, S1, B1, M1, T2),
    io:format("~w, ~w, ~w ~n", [S2, B2, M2]),
    D3 = dict:store(M2, T3, D2),
    {S3, B3, M3} = int_bs(D3, S2, B2, M2, T3),
    io:format("~w, ~w, ~w ~n", [S3, B3, M3]),

    D4 = dict:store(M3, T4, D3),
    {S4, B4, M4} = int_bs(D4, S3, B3, M3, T4),
    io:format("~w, ~w, ~w ~n", [S4, B4, M4]),

    D5 = dict:store(M4, T5, D4),
    {S5, B5, M5} = int_bs(D5, S4, B4, M4, T5),

    io:format("~w, ~w, ~w ~n", [S5, B5, M5]).

int_bs(Dict, Small, Big, Mid, Throughput) ->
    SmallTh = dict:fetch(Small, Dict),
    io:format("Small th is ~w, th is ~w ~n", [SmallTh, Throughput]),
    case Throughput > SmallTh of
        true ->
            S1 = Mid,
            M1 = (S1 + Big) div 2,
            case M1 of 
                Mid -> {S1, Big, Big};
                _ -> {S1, Big, M1}
            end;
        false ->
            B1 = Mid,
            M1 = (Small + B1) div 2,
            case M1 of 
                Mid -> {Small, B1, Small};
                _ -> {Small, B1, M1}
            end
    end.

check1(Key, Times, Dict) ->
    Seq = lists:seq(1, Times),
    lists:foreach(fun(_) -> {ok, {V1, V2, V3}}= dict:find(Key, Dict),
                    dict:store(Key, {V1, V2, V3}, Dict) end, Seq),
    ets:lookup(table, Key),
    Dict.

check2(Key, Times) ->
    Seq = lists:seq(1, Times),
    lists:foreach(fun(_) -> [{Key, {V1, V2, V3, V4}}]=  ets:lookup(table, Key),
                    ets:insert(table, {Key, {V1, V2, V3, V4}}) end, Seq).
    


init(NumItems) ->
    Seq = lists:seq(1, NumItems),
    lists:foreach(fun(X) -> ets:insert(table, {X, {X, 2*X, 3*X, "123456789014567890123457890123456789012345678901234567890123456789012345678901234567890"}}) end, Seq),
    lists:foldl(fun(X, D) ->     
                        dict:store(X, {X, 2*X, 3*X}, D) end, dict:new(), Seq).

random_string(Len) ->
    Chrs = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    ChrsSize = size(Chrs),
    F = fun(_, R) -> [element(random:uniform(ChrsSize), Chrs) | R] end,
    lists:foldl(F, "", lists:seq(1, Len)).


