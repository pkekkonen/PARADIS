-module(gen_worker).
-export([start/2, stop/1, async/2, await/1, await_all/1]).

-callback handle_work(State :: term()) ->  NewState :: term().

% Start a work-pool with Callback (module?) and Max processes handling the
% work. Pid is the process identifier of the work-pool.
start(Callback, Max) ->
    Pid = spawn(fun () ->

        Pids = [spawn_work() || I <- lists:seq(1,Max)],

		loop(Callback, Pids, 1, Max)
		end),
    Pid.

%DO we really need callback
loop(Callback, Pids, Current, Max) ->
	receive
		{to_master, Pid, {Ref, W}} ->
			lists:nth(Current, Pids) ! {process, Pid, {Ref, Callback, W}}, 
			if
			Current >= Max ->
				loop(Callback, Pids, 1, Max);
			true ->	
				loop(Callback, Pids, Current+1, Max)
			end;
		{stop, Pid} ->
			stop_worker(Pids),
			Pid ! {ok}
	end.

stop_worker([Pid|Pids]) ->
	exit(Pid, ok),
	stop_worker(Pids);
stop_worker([]) -> ok.

spawn_work() ->
    Pid = spawn(fun worker/0),
    Pid.

worker() ->
    receive
	{process, To, {Ref, Callback, W}} ->

	% TODO: handle work ska vara inom try catch
	    To ! {result, self(), {Ref, Callback:handle_work(W)}},
	    worker()
    end.

% Stop Pid and all its workers
stop(Pid) ->
    Pid ! {stop, self()},
    receive
    	{ok} ->
    		exit(Pid, ok)
    end.


% Schedule W for processing one worker at Pid. Return Ref, Ref is a unique
% reference that can be used to await(Ref) the result.
async(Pid, W) ->
    Ref = make_ref(),
    Pid ! {to_master, self(), {Ref, W}}, 
    Ref.


%TODO: error/no_result
% Await the result with the unique reference Ref created
% by async(Pid, W). Returns no result, error or {result, Result}.
await(Ref) ->
	receive
	{result, Pid, {Ref, Result}} ->
		Result
	end.

% Await the work for all references in the list Refs and return a (possibly empty) 
% list of results. Work resulting in no result should not be included in the list.
await_all(Refs) ->

    [ format_result(Res) || Res<- [await(Ref) || Ref <- Refs]].


format_result(Res) ->
	case Res of
		{result, Result} -> Result;
		R -> R
	end.

