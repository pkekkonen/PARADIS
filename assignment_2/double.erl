% Write an Erlang module double containing the function start/0. The
% function start/0 creates a process and registers it under the name double.
% The double-process accepts messages on the form Pid, Ref, N and returns a message with N doubled. 
% If the process receives a non-number it should crash.

% TODO: should be able to send twice?
-module(double).
-export([start/0]).

start() ->
	Pid = spawn(fun double/0),
	register(double, Pid).

double() ->
	receive
	{Pid, Ref, N} ->
		Pid ! {Ref, 2*N},
		double()
	end.