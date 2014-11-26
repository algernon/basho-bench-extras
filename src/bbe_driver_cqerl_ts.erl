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
                 use_prepared,
                 date_gen, time_gen,
                 numval_gen }).

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
    Prepared     = basho_bench_config:get(cassandra_use_prepared, false),
    DateGen      = basho_bench_config:get(cassandra_dategen),
    TimeGen      = basho_bench_config:get(cassandra_timegen),
    NumValGen    = basho_bench_config:get(cassandra_numvalgen),

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

    case use_keyspace (C, Keyspace) of
        ok ->
            {ok, #state { client = C,
                          keyspace = Keyspace,
                          columnfamily = ColumnFamily,
                          use_prepared = Prepared,

                          date_gen = basho_bench_keygen:new(DateGen, Id),
                          time_gen = basho_bench_keygen:new(TimeGen, Id),
                          numval_gen = basho_bench_keygen:new(NumValGen, Id)}};
        {error, Reason} ->
            ?FAIL_MSG("Failed to use keyspace ~p: ~p\n", [Keyspace, Reason])
    end.

run(ts_insert, KeyGen, _ValueGen,
    #state{client = C, columnfamily = ColumnFamily,
           date_gen = DateGen, numval_gen = NumValGen,
           time_gen = TimeGen,
           use_prepared = true} = State) ->

    Statement = "INSERT INTO " ++ ColumnFamily ++
        " (topic, date, time, numericvalue)" ++
        " VALUES (?, ?, ?, ?);",
    Query = #cql_query{statement = Statement,
                       consistency = ?CQERL_CONSISTENCY_ANY,
                      reusable = true},
    Values = [{topic, KeyGen()},
              {date, DateGen()},
              {time, TimeGen()},
              {numericvalue, NumValGen()}],

    case cqerl:run_query(C, Query#cql_query{values = Values}) of
        {ok, void} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Error} ->
            {error, Error, Statement, Values, State}
    end;

run(ts_insert, KeyGen, _ValueGen,
    #state{client = C, columnfamily = ColumnFamily,
           date_gen = DateGen, numval_gen = NumValGen,
           time_gen = TimeGen,
           use_prepared = false} = State) ->

    Statement = "INSERT INTO " ++ ColumnFamily ++
        " (topic, date, time, numericvalue)" ++
        " VALUES ('" ++ KeyGen() ++ "', " ++
        integer_to_list(DateGen()) ++
        ", " ++ TimeGen() ++ ", " ++
        integer_to_list(NumValGen()) ++ ");",
    Query = #cql_query{statement = Statement,
                       consistency = ?CQERL_CONSISTENCY_ANY,
                      reusable = true},

    case cqerl:run_query(C, Query) of
        {ok, void} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Error} ->
            {error, Error, Statement, State}
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
