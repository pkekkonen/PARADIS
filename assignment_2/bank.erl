

-module(bank).
-export([start/0, balance/2, deposit/3, withdraw/3, lend/4]).

% TODO: ska använda monitor/2 and make sure that it returns 
% no_bank when the Pid sent is not valid

% Start a new bank-server with all accounts having zero balance, returns a
% pid to the server.
start() ->
    spawn(fun () -> 
		Pid = spawn(fun bank_server/0),
		MRef = monitor(process, Pid),
		receive
	    	{'DOWN', MRef, process, Pid, _Why} ->
			monitor()
		end,
	end).

bank_server() ->
    ets:new(bank_server, [set,private,named_table]),
    loop().

loop() ->
    receive
	{Pid, Ref, {balance, Who}} ->
	    case ets:lookup(bank_server, Who) of
    	[] -> 
    		Pid ! {no_account, Ref};
    	[{Who, Balance}] ->
    		Pid ! {ok, Ref, Balance} 
	    end;
	{_Pid, _Ref, stop} ->
	    ok;
	{Pid, Ref, {deposit, {Who, X}}} ->
	    case ets:lookup(bank_server, Who) of
		[] ->
			ets:insert(bank_server, {Who, X}),
		    Pid ! {ok, Ref, X};
		[{Who, OldValue}] ->
			ets:insert(bank_server, {Who, OldValue+X}),
		    Pid ! {ok, Ref, OldValue+X}
	    end;
	{Pid, Ref, {withdraw, {Who, X}}} ->
	    case ets:lookup(bank_server, Who) of
		[] ->
		    Pid ! {Ref, insufficient_funds};
		[{Who, OldValue}] when OldValue >= X ->
			ets:insert(bank_server, {Who, OldValue-X}),
		    Pid ! {ok, Ref, OldValue-X};
		[{Who, OldValue}] when OldValue < X ->
		    Pid ! {Ref, insufficient_funds}	
	    end;
	{Pid, Ref, {lend, {From, To, X}}} ->
	    case {ets:lookup(bank_server, From), ets:lookup(bank_server, To)} of
		{[], []} ->
		    Pid ! {Ref, no_account, both};
		{[_], []} ->
		    Pid ! {Ref, no_account, To};
		{[], [_]} ->
		    Pid ! {Ref, no_account, From};
		{[{From, FromsOldValue}], [{To, TosOldValue}]} when FromsOldValue >= X ->
			ets:insert(bank_server, {From, FromsOldValue-X}),
			ets:insert(bank_server, {To, TosOldValue+X}),
		    Pid ! {Ref, ok};
		{[{From, FromsOldValue}], [{To, _TosOldValue}]} when FromsOldValue < X ->
		    Pid ! {Ref, insufficient_funds}	
	    end
    end,
    loop().


% Return the balance of Who from the server Pid. Return ok or no account.
balance(Pid, Who) when is_pid(Pid) ->
    Ref = make_ref(),
    Pid ! {self(), Ref, {balance, Who}},
    receive
	{ok, Ref, Balance} ->
	    {ok, Balance};
	{no_account, Ref} ->
	    no_account
    after 1000 ->
	    error
    end;
balance(_Pid, _Who) -> no_bank.

% Deposit X amount of money to the account Who at the server Pid. 
% If no account exists a new account for Who is opened. 
% Returns {ok, NewAmount}.
deposit(Pid, Who, X) when is_pid(Pid) ->
    Ref = make_ref(),
    Pid ! {self(), Ref, {deposit, {Who, X}}},
    receive
	{ok, Ref, NewAmount} ->
	    {ok, NewAmount}
    after 1000 ->
	    error
    end;
deposit(_Pid, _Who, _X) -> no_bank.

% Withdraw X amount of money from the account Who at the server Pid. 
% Returns {ok, AmountLeft} or insufficient funds.
withdraw(Pid, Who, X) when is_pid(Pid) ->
    Ref = make_ref(),
    Pid ! {self(), Ref, {withdraw, {Who, X}}},
    receive
	{Ref, insufficient_funds} ->
	    insufficient_funds;
	{ok, Ref, AmountLeft} ->
	    {ok, AmountLeft}
    after 1000 ->
	    error
    end;
withdraw(_Pid, _Who, _X) -> no_bank.


% Lend X amounts of money from From to To. 
% Return ok, insufficient funds or {no account, Who}, 
% where Who is the account that does not exist or the atom both if neither account exists.
lend(Pid, From, To, X) when is_pid(Pid) ->
    Ref = make_ref(),
    Pid ! {self(), Ref, {lend, {From, To, X}}},
    receive
	{Ref, insufficient_funds} ->
	    insufficient_funds;
	{Ref, no_account, Who} ->
	    {no_account, Who};
	{Ref, ok} ->
	    ok	    
    after 1000 ->
	    error
    end;
lend(_Pid, _From, _To, _X) -> no_bank.










