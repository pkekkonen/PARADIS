% Write an Erlang module double containing the function start/0. The
% function start/0 creates a process and registers it under the name double.
% The double-process accepts messages on the form Pid, Ref, N and returns a message with N doubled. 
% If the process receives a non-number it should crash.

-module(double).
-export([start/0, start_link/0, double/1]).

start() ->
	Pid = spawn(fun double/0),
	register(double, Pid).


start_link() ->
    Pid = spawn_link(fun double/0),
    register(?MODULE, Pid),
    {ok, Pid}.

double() ->
	receive
	{Pid, Ref, N} ->
		Pid ! {Ref, 2*N},
		double()
	end.

double(T) ->
	Ref = make_ref(), 
	?MODULE ! {self(), Ref, T},

	receive
		{Ref, N} -> self() ! {Ref, 2*N}
		after 1000 -> ?MODULE ! {self(), Ref, T}
	end.
