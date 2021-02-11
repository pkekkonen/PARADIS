-module(ring).
-export([start/2]).

% Creates a ring of N processes, then sends an integer M times round the ring. 
% The message starts as the integer 0 and each process in the ring increments the integer by 1.
start(N, M) ->
	Pids = [spawn(fun worker/0) || _ <- lists:seq(1,N)],
	Ref = make_ref(),
	lists:nth(1, Pids) ! {self(), Pids, M, Ref, {1, 1, 0}},
	receive
		{result, Ref, Result} -> 
			[exit(Pid, ok) || Pid <- Pids],
			Result
	end.


worker() ->
	receive 
		{MasterPid, Pids, M, Ref, {CurrentPidIndex, CurrentTurn, CurrentRes}} ->
			case {CurrentPidIndex, CurrentTurn} of
				{CPI, CT} when CPI =:= length(Pids) andalso CT =:= M -> 
					MasterPid ! {result, Ref, CurrentRes+1};
				{_CPI, CT} when CT =:= M -> 
					lists:nth(CurrentPidIndex+1, Pids) ! {MasterPid, Pids, M, Ref, {CurrentPidIndex+1, CurrentTurn, CurrentRes+1}};
				{CPI, _CT} when CPI =:= length(Pids) -> 
					lists:nth(1, Pids) ! {MasterPid, Pids, M, Ref, {1, CurrentTurn+1, CurrentRes+1}},
					worker();
				{_CPI, _CT} -> 
					lists:nth(CurrentPidIndex+1, Pids) ! {MasterPid, Pids, M, Ref, {CurrentPidIndex+1, CurrentTurn, CurrentRes+1}},
					worker()
			end
	end.				