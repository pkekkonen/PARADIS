% Implement the monitor-module from Assignment 2 Problem 1 using the
% supervisor behavior. The functionality should be exactly the same, i.e.,
% the monitor module starts the double process and monitors it, restarting
% it if it crashes. Note that only monitor.erl should be submitted!

-module(monitor).
-export([init/1, start_link/0]).
-behavior(supervisor).


start_link() ->
    supervisor:start_link(?MODULE, []).

init(_) ->
	SupFlags = #{strategy => one_for_one,
			 	intensity => 10, 
		 		period => 5},
    ChildSpec = [#{id => double_id,
		   start => {double, start_link, []}}],
    {ok, {SupFlags, ChildSpec}}.