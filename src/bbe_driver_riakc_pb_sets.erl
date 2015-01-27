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

-module(bbe_driver_riakc_pb_sets).

-export([new/1, run/4]).

-include("deps/basho_bench/include/basho_bench.hrl").

-record(state, { pid, type,
                 bucket_generator, batch_size_generator,
                 options }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips       = basho_bench_config:get(riakclient_sets_ips, [{127,0,0,1}]),
    Port      = basho_bench_config:get(riakclient_sets_port, 8087),
    Type      = basho_bench_config:get(riakclient_sets_type, <<"sets">>),
    BGen      = basho_bench_config:get(bucket_generator,
                                       {concat_binary, <<"bucket_">>,
                                        {to_binstr, "~b", {uniform_int, 1000}}}),
    BucketGen = basho_bench_keygen:new(BGen, Id),
    BSGenSpec = basho_bench_config:get(batch_size_generator,
                                       {pareto_int, 10}),
    BSGen     = basho_bench_keygen:new(BSGenSpec, Id),
    Options   = basho_bench_config:get(riakclient_sets_options, []),

    Targets = basho_bench_config:normalize_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),

    case riakc_pb_socket:start_link(TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            case riakc_pb_socket:get_bucket_type(Pid, Type) of
                {ok, _} ->
                    {ok, #state { pid = Pid,
                                  type = Type,
                                  bucket_generator = BucketGen,
                                  batch_size_generator = BSGen,
                                  options = Options
                                }};
                {error, Reason1} ->
                    ?FAIL_MSG("Error checking bucket type ~p's properties: ~p\n",
                              [Type, Reason1])
            end;
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

run(set_append_only, KeyGen, ValueGen,
    #state{batch_size_generator = BSGen, bucket_generator = BucketGen,
           pid = Pid, type = Type, options = Options} = State) ->
    N = BSGen() + 1,
    Set = lists:foldr(fun (X, Acc) ->
                              riakc_set:add_element(X, Acc)
                      end,
                      riakc_set:new(),
                      lists:map (fun (_) ->
                                         ValueGen()
                                 end, lists:seq(1, N))),

    case riakc_pb_socket:update_type(Pid,
                                     { Type, BucketGen() },
                                     KeyGen(),
                                     riakc_set:to_op(Set),
                                     Options) of
        ok ->
            {ok, State};
        {error, disconnected} ->
            run(set_append_only, KeyGen, ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end;


run(set_discard_only, KeyGen, ValueGen,
    #state{bucket_generator = BucketGen,
          pid = Pid, type = Type, options = Options} = State) ->
  Bucket = BucketGen(),
  Key =KeyGen(),

  {ok, Set} = case riakc_pb_socket:fetch_type(Pid, {Type,Bucket}, Key) of
                {error,{notfound,set}} -> {ok, riakc_set:new()};
                {ok, S} -> {ok, S}
  end,

  case riakc_set:size(Set) > 0 of
      false -> {ok, State};
      true ->
      % delete last element of the set
      ModSet=riakc_set:del_element(lists:last(ordsets:to_list(riakc_set:value(Set))), Set),
      % persist
      case riakc_pb_socket:update_type(Pid, { Type, Bucket }, Key, riakc_set:to_op(ModSet), Options) of
        ok ->
          {ok, State};
        {notfound, set} ->
          {ok, State};
        {error, {notfound, set}} ->
          {ok, State};
        {error, disconnected} ->
          run(set_discard_only, KeyGen, ValueGen, State);
        {error, Reason} ->
          {error, Reason, State}
      end
  end;

run(set_get, KeyGen, ValueGen,
    #state{bucket_generator = BucketGen, pid = Pid, type = Type,
           options = Options} = State) ->
    case riakc_pb_socket:fetch_type(Pid,
                                    { Type, BucketGen() },
                                    KeyGen(),
                                    Options) of
        {ok, _} ->
            {ok, State};
        {error, {notfound, set}} ->
            {ok, State};
        {error, disconnected} ->
            run(set_get, KeyGen, ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).
