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

-module(bbe_driver_riakc_pb_lmi).

-export([new/1,
         run/4]).

-include("deps/basho_bench/include/basho_bench.hrl").

-record(state, { pid, type,
                 location_generator, bucket_size,
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
    LGenSpec  = basho_bench_config:get(location_generator),
    LGen      = lmi_gen:new(LGenSpec, Id),
    BuckSize  = basho_bench_config:get(batch_size, 10),
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
                                  location_generator = LGen,
                                  bucket_size = BuckSize,
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

run(bucket_set_append, KeyGen, ValueGen,
    #state{location_generator = LocationGen, bucket_size = BucketSize,
           pid = Pid, type = Type, options = Options} = State) ->
    Set = lists:foldr(fun (X, Acc) ->
                              riakc_set:add_element(X, Acc)
                      end,
                      riakc_set:new(),
                      lists:map (fun (_) ->
                                         ValueGen()
                                 end, lists:seq(1, BucketSize))),
    {Bucket, Key} = LocationGen(),

    case riakc_pb_socket:update_type(Pid,
                                     { Type, Bucket },
                                     Key, riakc_set:to_op(Set),
                                     Options) of
        ok ->
            {ok, State};
        {error, disconnected} ->
            run(bucket_set_append, KeyGen, ValueGen, State);
        {error, Reason} ->
            {error, Reason, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).
