-module(pmap).
-export([unordered/2, unordered/3]).


unordered(Fun, L) ->
    Pids = [spawn_work(I, Fun) || I <- L],
    gather(Pids).

spawn_work(I, Fun) ->
    Pid = spawn(fun worker/0),
    Pid ! {self(), {work, I, Fun}},
    Pid.

spawn_work(I, Fun, Pid) ->
    Pid ! {self(), {work, I, Fun}},
    [Pid].

worker() ->
    receive
	{Master, {work, I, Fun}} ->
	    Master ! {self(), {result, Fun(I)}},
	    worker() %TODO: se till att processerna dör när klara
	% {Master, {work, I, Fun}, done} ->
	% 	Master ! {self(), {result, Fun(I)}}
    end.

gather([]) ->
    [];
gather([Pid|Pids]) ->
    receive
	{Pid, {result, R}} ->
	    [R | gather(Pids)]
    end.

quitProcesses([]) ->
    [];
quitProcesses([Pid|Pids]) ->
	exit(Pid, ok),
	quitProcesses(Pids).

unordered(Fun, L, MaxWorkers) ->
	if
	length(L) > MaxWorkers -> 
		{H, T} = lists:split(MaxWorkers, L),
    	Pids = [spawn_work(I, Fun) || I <- H],
    	% gather(Pids)
		unordered(Fun, T, MaxWorkers, Pids, 1);
	true ->
    	Pids = [spawn_work(I, Fun) || I <- L],
    	gather(Pids)
    end.



unordered(Fun, L, MaxWorkers, Pids, Current) -> 
    case L of
    	[] -> 
    		Res = gather(Pids),
    		{UniquePids, _} = lists:split(MaxWorkers, Pids),
			quitProcesses(UniquePids),
			Res;
    	[H|T] when MaxWorkers =:= Current+1 -> 
    		NewPids = Pids++spawn_work(H, Fun, lists:nth(Current, Pids)),
			unordered(Fun, T, MaxWorkers, NewPids, 1);
    	[H|T] -> 
    		NewPids = Pids++ spawn_work(H, Fun, lists:nth(Current, Pids)),
			unordered(Fun, T, MaxWorkers, NewPids, Current+1)
    end.










