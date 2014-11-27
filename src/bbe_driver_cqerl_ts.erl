%% -------------------------------------------------------------------
%%
%% basho-bench-extras: basho_bench extensions
%%
%% Copyright (c) 2014 Gergely Nagy <algernon@madhouse-project.org>
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

-module(bbe_driver_cqerl_ts).

-export([new/1,
         run/4]).

-include("deps/basho_bench/include/basho_bench.hrl").
-include("deps/cqerl/include/cqerl.hrl").

-record(state, { client, keyspace, columnfamily,
                 date_gen, time_gen, min_timediff }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    case code:which(cqerl) of
        non_existing ->
            ?FAIL_MSG("~s requires cqerl module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Host         = basho_bench_config:get(cassandra_host, "localhost"),
    Port         = basho_bench_config:get(cassandra_port, 9042),
    Keyspace     = basho_bench_config:get(cassandra_keyspace, "Keyspace1"),
    ColumnFamily = basho_bench_config:get(cassandra_columnfamily, "ColumnFamily1"),
    ReplFactor   = basho_bench_config:get(cassandra_keyspace_replication_factor, 1),
    DateGen      = basho_bench_config:get(cassandra_dategen),
    TimeGen      = basho_bench_config:get(cassandra_timegen),
    MinTimeDiff  = basho_bench_config:get(cassandra_select_min_timediff, 10),

    case erlang:function_exported(application, ensure_all_started, 1) of
        true -> application:ensure_all_started(cqerl);
        false ->
            application:start(crypto),
            application:start(asn1),
            application:start(public_key),
            application:start(ssl),
            application:start(pooler),
            application:start(cqerl)
    end,

    {ok, C} = cqerl:new_client({Host, Port}),
    ?INFO("ID: ~p, Connected to Cassandra at ~p:~p\n", [Id, Host, Port]),

    case bootstrap_keyspace (C, Keyspace, ColumnFamily, ReplFactor) of
        ok ->
            {ok, #state { client = C,
                          keyspace = Keyspace,
                          columnfamily = ColumnFamily,

                          date_gen = basho_bench_keygen:new(DateGen, Id),
                          time_gen = basho_bench_keygen:new(TimeGen, Id),
                          min_timediff = MinTimeDiff}};
        {error, Reason} ->
            ?FAIL_MSG("Failed to bootstrap keyspace ~p: ~p\n", [Keyspace, Reason])
    end.

run(ts_insert, KeyGen, ValueGen,
    #state{client = C, columnfamily = ColumnFamily,
           date_gen = DateGen, time_gen = TimeGen} = State) ->

    Statement = "INSERT INTO " ++ ColumnFamily ++
        " (topic, date, time, padding)" ++
        " VALUES (?, ?, mintimeuuid(?), ?);",
    Query = #cql_query{statement = Statement,
                       consistency = ?CQERL_CONSISTENCY_ANY,
                       reusable = true},
    Values = [{topic, KeyGen()},
              {date, DateGen()},
              {'arg0(mintimeuuid)', TimeGen()},
              {padding, binary_to_list(ValueGen())}],

    case cqerl:run_query(C, Query#cql_query{values = Values}) of
        {ok, void} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Error} ->
            {error, Error, Statement, Values, State}
    end;
run(ts_select, KeyGen, _ValueGen,
   #state{client = C, columnfamily = ColumnFamily,
          date_gen = DateGen, time_gen = TimeGen,
          min_timediff = MinTimeDiff} = State) ->
    Statement = "SELECT topic, date, time FROM " ++ ColumnFamily ++
        " WHERE topic = ? AND date = ? AND " ++
        "time > minTimeuuid(?) AND time < maxTimeuuid(?);",
    Query = #cql_query{statement = Statement,
                       consistency = ?CQERL_CONSISTENCY_ONE,
                       reusable = true},
    {MinTime, MaxTime} = generate_time_range(TimeGen, {MinTimeDiff}),
    Values = [{topic, KeyGen()},
              {date, DateGen()},
              {'arg0(mintimeuuid)', MinTime},
              {'arg0(maxtimeuuid)', MaxTime}],
    case cqerl:run_query(C, Query#cql_query{values = Values}) of
        {ok, void} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Error} ->
            {error, Error, Statement, Values, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

use_keyspace(C, Keyspace) ->
    case cqerl:run_query(C, lists:concat(["USE ", Keyspace, ";"])) of
        {ok, _} ->
            ok;
        {error, not_ready} ->
            timer:sleep(100),
            use_keyspace(C, Keyspace);
        {error, Reason} ->
            {error, Reason}
    end.

bootstrap_keyspace(C, Keyspace, ColumnFamily, ReplicationFactor) ->
    CreateKeySpaceStmt = "CREATE KEYSPACE \"" ++ Keyspace ++ "\" " ++
        "WITH REPLICATION = " ++
        "{'class': 'SimpleStrategy'," ++
        " 'replication_factor': " ++ integer_to_list(ReplicationFactor) ++ "};",
    ?DEBUG("~p => ~p\n", [CreateKeySpaceStmt, cqerl:run_query(C, CreateKeySpaceStmt)]),
    use_keyspace(C, Keyspace),
    CreateTableStmt = "CREATE TABLE " ++ ColumnFamily ++ " " ++
        "(Topic text, Date int, Time timeuuid, Padding text, " ++
        " PRIMARY KEY ((Topic, Date), Time)) " ++
        "WITH CLUSTERING ORDER BY (Time Asc);",
    ?DEBUG("~p => ~p\n", [CreateTableStmt, cqerl:run_query(C, CreateTableStmt)]),
    ok.

generate_time_range(TimeGen, {MinTimeDiff}) ->
    generate_time_range(TimeGen, {TimeGen(), MinTimeDiff});
generate_time_range(TimeGen, {Minimum, MinTimeDiff}) ->
    Time = TimeGen(),
    if abs(Time - Minimum) =< MinTimeDiff ->
            generate_time_range(TimeGen, {Minimum, MinTimeDiff});
       true ->
            {Minimum, Time}
    end.
