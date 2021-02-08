% -module(pmap).
% -export([unordered/2, unordered/3, ordered/3]).


% unordered(Fun, L) ->
%     Pids = [spawn_work(I, Fun) || I <- L],
%     gather(Pids).

% spawn_work(I, Fun) ->
%     Pid = spawn(fun worker/0),
%     Pid ! {self(), {work, I, Fun}},
%     Pid.

% spawn_work(I, Fun, Pid) ->
%     Pid ! {self(), {work, I, Fun}},
%     [Pid].

% worker() ->
%     receive
% 	{Master, {work, I, Fun}} ->
% 	    Master ! {self(), {result, Fun(I)}},
% 	    worker() %TODO: se till att processerna dör när klara
% 	% {Master, {work, I, Fun}, done} ->
% 	% 	Master ! {self(), {result, Fun(I)}}
%     end.

% gather([]) ->
%     [];
% gather([Pid|Pids]) ->
%     receive
% 	{Pid, {result, R}} ->
% 	    [R | gather(Pids)]
%     end.

% quitProcesses([]) ->
%     [];
% quitProcesses([Pid|Pids]) ->
% 	exit(Pid, ok),
% 	quitProcesses(Pids).

% unordered(Fun, L, MaxWorkers) ->
% 	if
% 	length(L) > MaxWorkers -> 
% 		{H, T} = lists:split(MaxWorkers, L),
%     	Pids = [spawn_work(I, Fun) || I <- H],
%     	% gather(Pids)
% 		unordered(Fun, T, MaxWorkers, Pids, 1);
% 	true ->
%     	Pids = [spawn_work(I, Fun) || I <- L],
%     	gather(Pids)
%     end.



% unordered(Fun, L, MaxWorkers, Pids, Current) -> 
%     case L of
%     	[] -> 
%     		Res = gather(Pids),
%     		{UniquePids, _} = lists:split(MaxWorkers, Pids),
% 			quitProcesses(UniquePids),
% 			Res;
%     	[H|T] when MaxWorkers =:= Current+1 -> 
%     		NewPids = Pids++spawn_work(H, Fun, lists:nth(Current, Pids)),
% 			unordered(Fun, T, MaxWorkers, NewPids, 1);
%     	[H|T] -> 
%     		NewPids = Pids++ spawn_work(H, Fun, lists:nth(Current, Pids)),
% 			unordered(Fun, T, MaxWorkers, NewPids, Current+1)
%     end.

% ordered(Fun, L, MaxWorkers) -> unordered(Fun, L, MaxWorkers).



-module(pmap).
-behaviour(gen_worker).
-export([handle_work/1, ordered/2]).
%% Simple handle_work: apply the function to the value
handle_work({Fun, V}) ->
{result, Fun(V)}.
ordered(Fun, L) ->
%% Start a work-pool with 2 workers
WorkPool = gen_worker:start(?MODULE, 2),
%% Schedule the work asynchronously
Refs = [gen_worker:async(WorkPool, {Fun, V}) || V <- L],
%% Await the result
Result = gen_worker:await_all(Refs),
%% Stop our work pool
gen_worker:stop(WorkPool),
%% Return the result
Result.


42> pmap:ordered(fun (X) -> when  X > 4 ->  2 end, [2,3,5,2,6]).  
* 10: syntax error before: 'when'
42> pmap:ordered(fun (X) when  X > 4 ->  2 end, [2,3,5,2,6]).   
[]
43> pmap:ordered(fun (X) -> X*2 end, [1,2,3]).                                  []                                                                              44> c(pmap), c(gen_worker).                                                     {ok,gen_worker}                                                                 45> pmap:ordered(fun (X) -> X*2 end, [1,2,3]).                                  [no_result,no_result,no_result]                                                 46> c(pmap), c(gen_worker).                                                     {ok,gen_worker}                                                                 47> pmap:ordered(fun (X) -> X*2 end, [1,2,3]).                                                                                                                                                                 





% c(gen_worker), c(pmap).

