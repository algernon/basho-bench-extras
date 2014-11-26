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

-module(bbe_driver_seestar_ts).

-export([new/1,
         run/4]).

-include("deps/basho_bench/include/basho_bench.hrl").

-record(state, { client, keyspace, columnfamily,
                 date_gen, time_gen,
                 numval_gen }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    case code:which(seestar) of
        non_existing ->
            ?FAIL_MSG("~s requires seestar module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Host         = basho_bench_config:get(cassandra_host, "localhost"),
    Port         = basho_bench_config:get(cassandra_port, 9042),
    Keyspace     = basho_bench_config:get(cassandra_keyspace, "Keyspace1"),
    ColumnFamily = basho_bench_config:get(cassandra_columnfamily, "ColumnFamily1"),
    DateGen      = basho_bench_config:get(cassandra_dategen),
    TimeGen      = basho_bench_config:get(cassandra_timegen),
    NumValGen    = basho_bench_config:get(cassandra_numvalgen),

    {ok, C} = seestar_session:start_link(Host, Port),
    ?INFO("ID: ~p, Connected to Cassandra at ~p:~p\n", [Id, Host, Port]),

    case use_keyspace (C, Keyspace) of
        ok ->
            {ok, #state { client = C,
                          keyspace = Keyspace,
                          columnfamily = ColumnFamily,

                          date_gen = basho_bench_keygen:new(DateGen, Id),
                          time_gen = basho_bench_keygen:new(TimeGen, Id),
                          numval_gen = basho_bench_keygen:new(NumValGen, Id)}};
        {error, Reason} ->
            ?FAIL_MSG("Failed to use keyspace ~p: ~p\n", [Keyspace, Reason])
    end.

run(ts_insert, KeyGen, ValueGen,
    #state{client = C, columnfamily = ColumnFamily,
           date_gen = DateGen, numval_gen = NumValGen,
           time_gen = TimeGen} = State) ->

    Statement = "INSERT INTO " ++ ColumnFamily ++
        " (topic, date, time, numericvalue, category)" ++
        " VALUES (?, ?, mintimeuuid(?), ?, ?);",
    Values = [list_to_binary(KeyGen()),
              DateGen(),
              millis_to_timestamp(TimeGen()),
              NumValGen(),
              ValueGen()],

    case seestar_session:prepare(C, Statement) of
        {ok, Prepared} ->
            QId = seestar_result:query_id (Prepared),
            Types = seestar_result:types (Prepared),
            case seestar_session:execute (C, QId, Types, Values, one) of
                {ok, _} ->
                    {ok, State};
                {error, Error} ->
                    {error, Error, Statement, Values, State}
            end;
        {error, Error} ->
            {error, Error, Statement, Values, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

use_keyspace(C, Keyspace) ->
    case seestar_session:perform(C, lists:concat(["USE ", Keyspace, ";"]), one) of
        {ok, _} ->
            ok;
        {error, not_ready} ->
            timer:sleep(100),
            use_keyspace(C, Keyspace);
        {error, Reason} ->
            {error, Reason}
    end.

millis_to_timestamp(Millis) ->
    {Millis div 1000000000,
     (Millis div 1000) rem 1000000,
     (Millis * 1000) rem 1000000}.
