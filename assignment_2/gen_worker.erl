-module(gen_worker).
-export([start/2, stop/1, async/2, await/1, await_all/1]).

-callback handle_work(State :: term()) ->  NewState :: term().

% TODO: måste ets vara private???? i double

% Start a work-pool with Callback and Max processes handling the
% work. Pid is the process identifier of the work-pool.
start(Callback, Max) ->
    Pid = spawn(fun () ->
        Pids = [spawn_work() || _ <- lists:seq(1,Max)],
		loop(Callback, Pids, 1, Max)
		end),
    Pid.

loop(Callback, Pids, Current, Max) ->
	receive
		{to_master, Pid, {Ref, W}} ->
			lists:nth(Current, Pids) ! {process, Pid, {Ref, Callback, W}}, 
			if
			Current >= Max ->
				loop(Callback, Pids, 1, Max);
			true  ->	
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
		Result =
			try
				Callback:handle_work(W)
			catch
				_:_ -> error
			end,

	    To ! {result, {Ref, Result}},
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


% Await the result with the unique reference Ref created
% by async(Pid, W). Returns no result, error or {result, Result}.
await(Ref) ->
	receive
	{result, {Ref, Result}} ->
		Result
	after 1000 -> no_result
	end.

% Await the work for all references in the list Refs and return a (possibly empty) 
% list of results. Work resulting in no result should not be included in the list.
await_all(Refs) ->
    [ format_result(Res) || Res<- [await(Ref) || Ref <- Refs], Res =/= no_result, Res =/= error].


format_result({result, Result}) ->
	Result.

