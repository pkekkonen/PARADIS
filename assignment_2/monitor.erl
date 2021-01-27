% Write an Erlang module monitor containing the function start/0. The
% start/0 function should start the double process and monitor it. If
% the double process crashes, the monitor process should restart it and
% continue to monitor its execution. Finally, implement double:double/1,
% which takes as input an Erlang term and tries to double it using the double
% process, retrying if the double process is (temporarily) down due to the
% monitor process restarting it.

-module(monitor).
-import(double, [start/0]).
-export([start1/0]).

start1() ->
    Pid = spawn(fun monitor/0), 
	register(monitor, Pid).


monitor() ->    		
	double:start(),
   	Pid = whereis(double),
   	Ref = monitor(process, Pid),
	receive
	    {'DOWN', Ref, process, Pid, _Why} ->
		monitor()
	end.
