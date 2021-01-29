-module(gen_worker).
-behaviour(handle_work).
-export([start/2, stop/1, async/2, await/1, await_all/1]).

% TODO: use this to implement assign 3

% Start a work-pool with Callback and Max processes handling the
% work. Pid is the process identifier of the work-pool.
start(Callback, Max) ->

.

% Stop Pid and all its workers
stop(Pid) ->

.

% Schedule W for processing one worker at Pid. Ref is a unique
% reference that can be used to await(Ref) the result.
async(Pid, W) ->

.

% Await the result with the unique reference Ref created
% by async(Pid, W). Returns no result, error or {result, Result}.
await(Ref) ->

.

% Await the work for all references in the list Refs and return a (possibly empty) 
% ist of results. Work resulting in no result should not be included in the list.
await_all(Refs) ->

.