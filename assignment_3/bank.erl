-module(bank).
-export([init/1, start_link/0, start/0, handle_call/3, handle_info/2, handle_cast/2, balance/2, deposit/3, withdraw/3, lend/4, terminate/2]).
-behavior(gen_server).

start() -> 
    start_link().
    
start_link() ->
	{ok, Pid} = gen_server:start_link(?MODULE, [], []),
	spawn(fun () -> 
		MRef = monitor(process, Pid),
		receive
	    	{'DOWN', MRef, process, _Pid, _Why} ->
	    	    	io:format("HEJ"),
			% ets:delete(bank_server),
			no_bank,
			io:format("gtgtgt")
		end
	end),
	Pid.

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
	Response = gen_server:call(Pid, {get, Who}),
	case Response of
		no_account -> no_account;
		Amount -> {ok, Amount}
	end;
balance(_Pid, _Who) -> no_bank.	

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
	{_FromAmount, _ToAmount} ->
		case withdraw(Pid, From, X) of
			insufficient_funds -> 
				insufficient_funds;
			{ok, _} -> 
				deposit(Pid, To, X),
				ok
		end
    end;
lend(_Pid, _From, _To, _X) -> no_bank.




