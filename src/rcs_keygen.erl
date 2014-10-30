%% -------------------------------------------------------------------
%%
%% basho-bench-riak-sets: Riak Set benchmark plugin for basho_bench
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

-module(rcs_keygen).

-export([numbered_bin/3]).

numbered_bin(Id, Prefix, Generator) ->
    Gen = basho_bench_keygen:new(Generator, Id),
    fun() ->
            Suffix = list_to_binary(integer_to_list(Gen())),
            <<Prefix/binary,Suffix/binary>>
    end.
