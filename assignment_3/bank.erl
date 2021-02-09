% Implement the bank-module from Assignments 2 Problem 1 using the gen server
% behavior. The inferface should exactly correspond to that of Assignment 2.
% Note that you can reuse much of your application logic.

-module(bank).
-export([init/1, start_link/0, handle_call/3, handle_cast/2, balance/2, deposit/3, withdraw/3, lend/4]).
-behavior(gen_server).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

init(_) -> 
	ets:new(bank_server, [set, private, named_table]),
	{ok, no_state}.

handle_call({get, Who}, _From, State) ->
	Response = case ets:lookup(bank_server, Who) of
    	[] -> 
    		no_account;
    	[{Who, Balance}] ->
    		Balance 
	    end,
	{reply, Response, State}.

handle_cast({set, {Who, X}}, State) -> 
	ets:insert(bank_server, {Who, X}),
    {noreply, State}.

handle_info(_Info, State) ->
    io:format("Unknown message: ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ets:delete(bank_server),
    ok.


% INTERFACE 


% Return the balance of Who from the server Pid. Return ok or no account.
balance(Pid, Who) when is_pid(Pid)->	
	{ok, gen_server:call(Pid, {get, Who})};
balance(Pid, Who) -> no_bank.	

% Deposit X amount of money to the account Who at the server Pid. 
% If no account exists a new account for Who is opened. 
% Returns {ok, NewAmount}.
deposit(Pid, Who, X) when is_pid(Pid) ->
	Amount = case gen_server:call(Pid, {get, Who}) of 
		no_account ->
			X;
		Current ->
			Current + X
		end,
	gen_server:cast(Pid, {set, {Who, Amount}}),
	{ok, Amount};	
deposit(_Pid, _Who, _X) -> no_bank.

% Withdraw X amount of money from the account Who at the server Pid. 
% Returns {ok, AmountLeft} or insufficient funds.
withdraw(Pid, Who, X) when is_pid(Pid) ->
	case gen_server:call(Pid, {get, Who}) of 
		Current when is_integer(Current) andalso Current >= X ->
			gen_server:cast(Pid, {set, {Who, Current-X}}),
			{ok, Current-X};
		_ ->
			insufficient_funds
		end;
withdraw(_Pid, _Who, _X) -> no_bank.


% Lend X amounts of money from From to To. 
% Return ok, insufficient funds or {no account, Who}, 
% where Who is the account that does not exist or the atom both if neither account exists.
lend(Pid, From, To, X) when is_pid(Pid) ->
    case {gen_server:call(Pid, {get, From}), gen_server:call(Pid, {get, To})} of
	{no_account, no_account} ->
	    {no_account, both};
	{_, no_account} ->
	    {no_account, To};
	{no_account, _} ->
	    {no_account, From};
	{FromAmount, ToAmount} ->
		case withdraw(Pid, From, X) of
			insufficient_funds -> 
				insufficient_funds;
			{ok, _} -> 
				deposit(Pid, To, X),
				ok
		end
    end;
lend(_Pid, _From, _To, _X) -> no_bank.



