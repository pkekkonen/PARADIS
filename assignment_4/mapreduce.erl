-module(mapreduce).
-export([mapreduce/5, mapreduce/6, test/0, test_distributed/0]).

test() ->
    Mapper = fun (_Key, Text) ->
		     [{Word, 1} || Word <- Text]
	     end,
    Reducer = fun (Word, Counts) ->
		      [{Word, lists:sum(Counts)}]
	      end,
    mapreduce(Mapper, 2, Reducer, 1, [{a, ["hello", "world", "hello", "text"]}, {b, ["world", "a", "b", "text"]}]).
		      

mapreduce_seq(Mapper, Reducer, Input) ->       
    Mapped = [{K2,V2} || {K,V} <- Input, {K2,V2} <- Mapper(K,V)],    
    reduce_seq(Reducer, Mapped).

reduce_seq(Reduce,KVs) -> 
    [KV || {K,Vs} <- groupkeys(lists:sort(KVs)), KV <- Reduce(K,Vs)]. 

%% INPUT:  [{K1, V1}, {K1, V2}, {K2, V3}]
%% OUTPUT: [{K1, [V1, V2]}, {K2, [V3]}]
groupkeys([]) ->
    [];
groupkeys([{K, V}|Rest]) ->
    groupkeys(K, [V], Rest).

groupkeys(K, Vs, [{K, V}|Rest]) ->
    groupkeys(K, [V|Vs], Rest);
groupkeys(K, Vs, Rest) ->
    [{K, lists:reverse(Vs)}|groupkeys(Rest)].

%% INPUT: [a,b,c,d], 2
%% OUTPUT: [[a,b], [c,d]]
%% INPUT: [a, b], 2
%% OUTPUT: [[],[], [a], [b]]
partition(N, L) ->
    partition(N, L, length(L)).

partition(1, L, _) ->
    [L];
partition(N, L, Len) ->
    {Prefix, Suffix} = lists:split(Len div N, L),
    [Prefix | partition(N - 1, Suffix, Len - (Len div N))].

%% Partition tagged chunks to the same list 
%%
%% INPUT: [[{1, [{K1, V1}, {K2, V2}]}, {2, [{K3, V1}]}], [{2, [{K3, V10}]}]]
%% OUTPUT: [[{K1, V1}, {K2, V2}], [{K3,V1}, {K3, V10}]]
chunk(Data, Chunks) ->
    chunk(Data, Chunks + 1, 1).

chunk(_, Chunks, I) when Chunks=:= I ->
    [];
chunk(Data, Chunks, I) ->
    [[Value || Chunk <- Data, 
	       {MapKey, MapValue} <- Chunk, 
	       MapKey =:= I, 
	       Value <- MapValue]|chunk(Data, Chunks, I + 1)].

mapreduce(Mapper, Mappers, Reducer, Reducers, Input) ->
    Self = self(),
    Ref = make_ref(),
    Partitions = partition(Mappers, Input),

    MapperPids = [spawn_mapper(Self, Ref, Mapper, Reducer, Reducers, Part) || Part <- Partitions],
    ReducerPids = [receive
		      {reducer_pids, {Pid, Ref, RedPid}} ->
			  		RedPid
		  end || Pid <- MapperPids],

	UniqueReducerPids = [ whereis(P)||P<-lists:sort(remove_duplicates(lists:flatten(ReducerPids)))],


	%% TODO: how to know which Pids we should send done to? Is it smoother to save all the Pids in a lookup table

    %% Bring all data from the mappers togheter such that all keyvalue
    %% pairs with the same id is assigned to the same reducer
    % Chunks = chunk(MapperData, Reducers),
    % ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],

    % TODO: här skicka meddelanden till alla reducer pids och säg att vi är done så de kan processera datan de har fått
    [Pid ! {done, Reducers} || Pid <- UniqueReducerPids],



	% Output = get_data(UniqueReducerPids, [], Ref),
    Output = [receive 
		  {reduce, {Pid, Ref, Data}} ->
		      Data
	      end || Pid <- UniqueReducerPids],

    %% Flatten the output from the reducers
    %% sort becouse it looks nice :-)
    lists:sort(lists:flatten(Output)).

%% INPUT: [{DataKey, [DataValue1, ..., DataValueN]}]
spawn_mapper(Master, Ref, Mapper, Reducer, Reducers, Data) ->
    spawn_link(fun () ->
		       %% phash: hash erlang term to `Reducers` bins.
		       %% In this case we tag each value with the same
		       %% value so that they will be processed by the
		       %% same reducer.
		       %TODO: skicks istället här i
		       Map = [{erlang:phash(MapKey, Reducers), {MapKey, MapValue}} ||
				 %% For each element in Data
				 {DataKey, DataValue} <- Data, 
				 %% Apply the Mapper to the key and value
				 %% and iterate over those
				 {MapKey, MapValue} <- Mapper(DataKey, DataValue)],

		       % %% Use `groupkeys` to group each tagged tupe together
		       % Master ! {map, {self(), Ref, groupkeys(lists:sort(Map))}}
		       %ska skicka till reducer, kan registrera Pid som hashkod
		       

		       lists:foreach(fun({Hashcode, {MapKey, MapValue}}) ->
				 	Reference = list_to_atom(integer_to_list(Hashcode)),

				   case whereis(Reference) of
				   		undefined ->
				   			%skapa process, registrera 
				   			ReducerPid = spawn_reducer(Master, Ref, Reducer),
				   			register(Reference, ReducerPid),
				   			Reference ! {data, [{Hashcode, {MapKey, MapValue}}]};
				   		_ ->
				   			%skicka till den piden
				   			Reference ! {data, [{Hashcode, {MapKey, MapValue}}]}
				   	end
		       	end, Map),

		     		       	Master ! {reducer_pids, {self(), Ref, [list_to_atom(integer_to_list(Hashcode)) || {Hashcode, {_MapKey, _MapValue}}<-Map]}}



	       end).


remove_duplicates(L) -> sets:to_list(sets:from_list(L)).

    
%reduceraren kan inte börja arbeta fören den fått alla nycklar
spawn_reducer(Master, Ref, Reducer) ->
    spawn_link(fun () ->
    			Chunk = reducer_loop([]),
    			% TODO: se till att den verkligen stannar här och väntar tills loopen är klar


		       %% Group the values for each key and apply
		       %% The reducer to each K, value list pair
		       Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(lists:flatten(Chunk))),
				       KV <- Reducer(K,Vs)],

		       Master ! {reduce, {self(), Ref, Reduce}}

	       end).




reducer_loop(Data) ->
	io:format("reduce begin ~n"),
	receive
		{data, IncomingData} ->
			io:format("incoming data: ~p ~n", [IncomingData]),
			reducer_loop(Data ++ IncomingData);
		% {register, Reference} -> register(Reference, self());
		{done, Reducers} ->
			io:format("done~n"),
			chunk([groupkeys(lists:sort(lists:flatten(Data)))], Reducers)
	end.





spawn_mappers_w_nodes(_Self, _Ref, _Mapper, _Reducer, _Reducers, [], _Nodes, _CurrentNode, Res) -> Res;
spawn_mappers_w_nodes(Self, Ref, Mapper, Reducer, Reducers, [Part|Partitions], Nodes, CurrentNode, Res) ->
	Pid = spawn_mapper(CurrentNode, Nodes, Self, Ref, Mapper, Reducer, Reducers, Part),
	case CurrentNode + 1 > length(Nodes) of
		true ->
			spawn_mappers_w_nodes(Self, Ref, Mapper, Reducer, Reducers, Partitions, Nodes, 1, Res++[Pid]);
		false ->
			spawn_mappers_w_nodes(Self, Ref, Mapper, Reducer, Reducers, Partitions, Nodes, CurrentNode+1, Res++[Pid])
	end.


mapreduce(Nodes, Mapper, Mappers, Reducer, Reducers, Input) ->
    Self = {self(), node()},
    register(master, self()),

    Ref = make_ref(),
    Partitions = partition(Mappers, Input),

    MapperPids = spawn_mappers_w_nodes(Self, Ref, Mapper, Reducer, Reducers, Partitions, Nodes, 1, []),
    % io:format("DONE ~n"),
    ReducerPids = [receive
		      {reducer_pids, {Pid, Ref, RedPid}} ->
			  		RedPid
		  end || Pid <- MapperPids],
    % io:format("ReducerPids: ~p ~n", [remove_duplicates(lists:flatten(ReducerPids))]),



	%% TODO: how to know which Pids we should send done to? Is it smoother to save all the Pids in a lookup table

    %% Bring all data from the mappers togheter such that all keyvalue
    %% pairs with the same id is assigned to the same reducer
    % Chunks = chunk(MapperData, Reducers),
    % ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],

    % TODO: här skicka meddelanden till alla reducer pids och säg att vi är done så de kan processera datan de har fått
    [{list_to_atom(integer_to_list(Pid)), Node} ! {done, Reducers} || {Pid, Node} <- remove_duplicates(lists:flatten(ReducerPids))],
    
        io:format("FullPids ~p ~n", [remove_duplicates(lists:flatten(ReducerPids))]),

    ReducerPidsAndNodes = [{list_to_atom(integer_to_list(Pid)), Node} || {Pid, Node} <- remove_duplicates(lists:flatten(ReducerPids))],

    io:format("HEREEEE ~p ~n", [ReducerPidsAndNodes]),
    io:format("WAITING FOR ~p ~n", [[ {reduce, {PidAndNodeName, Ref, data}}|| PidAndNodeName<- ReducerPidsAndNodes]]),


	% Output = get_data(UniqueReducerPids, [], Ref),
    Output = [receive 
		  {reduce, {P, Ref, Data}} ->
		      Data
	      end || P<- ReducerPidsAndNodes],

    %% Flatten the output from the reducers
    %% sort becouse it looks nice :-)
    lists:sort(lists:flatten(Output)).

% get_pids([], Res) -> Res;
% get_pids([{Pid, Node}|ReducerPids], Res) -> 
% 	FullPid = rpc:call(Node, erlang, whereis, [list_to_atom(integer_to_list(Pid))]),
% 	case FullPid of
% 		undefined -> 
% 			io:format("Pid: ~p Address: ~p ~n", [Pid, whereis(list_to_atom(integer_to_list(Pid)))]),
% 			get_pids(ReducerPids, Res++[{whereis(list_to_atom(integer_to_list(Pid))), Node}]);
% 		P -> get_pids(ReducerPids, Res++[{P, Node}])
% 	end.




spawn_mapper(CurrentNode, Nodes, Master, Ref, Mapper, Reducer, Reducers, Data) ->
    spawn_link(lists:nth(CurrentNode, Nodes), fun () ->
		       %% phash: hash erlang term to `Reducers` bins.
		       %% In this case we tag each value with the same
		       %% value so that they will be processed by the
		       %% same reducer.
		       %TODO: skicks istället här i
		       Map = [{erlang:phash(MapKey, Reducers), {MapKey, MapValue}} ||
				 %% For each element in Data
				 {DataKey, DataValue} <- Data, 
				 %% Apply the Mapper to the key and value
				 %% and iterate over those
				 {MapKey, MapValue} <- Mapper(DataKey, DataValue)],

		       % %% Use `groupkeys` to group each tagged tupe together
		       % Master ! {map, {self(), Ref, groupkeys(lists:sort(Map))}}
		       %ska skicka till reducer, kan registrera Pid som hashkod
		       
		       % io:format("NODE: ~p ~n", [node()]),
		       % io:format("Map: ~p ~n", [Map]),
		       lists:foreach(fun({Hashcode, {MapKey, MapValue}}) ->

				 	Reference = list_to_atom(integer_to_list(Hashcode)),
				 	Node = lists:nth(erlang:phash(Hashcode, length(Nodes)), Nodes),
				   	% io:format("HASH: ~p Node: ~p MK: ~p MV: ~p Reg: ~p ~n", [Reference, Node, MapKey, MapValue, rpc:call(Node, erlang, whereis, [Reference])]),
		       io:format("Ref: ~p ~n", [Reference]),

				   case rpc:call(Node, erlang, whereis, [Reference]) of
				   		undefined ->
				   			%skapa process, registrera 
				   			ReducerPid = spawn_reducer(Node, Master, Ref, Reducer, Reference),

				   			% io:format("ReducerPid: ~p REDUCERNODE: ~p ONNODE: ~p ~n", [ReducerPid, Node, node()]),

				   			if
				   				Node =:= node() ->
				   						   			% io:format("SAME NODE ReducerPid: ~p REDUCERNODE: ~p ONNODE: ~p ~n", [ReducerPid, Node, node()]),
				   						       		% io:format("1  HASH: ~p Node: ~p MK: ~p MV: ~p ~n", [Reference, Node, MapKey, MapValue]),

				   					ReducerPid ! {data, [{Hashcode, {MapKey, MapValue}}]};
				   				true ->
				   						       		% io:format("2  HASH: ~p Node: ~p MK: ~p MV: ~p ~n", [Reference, Node, MapKey, MapValue]),


						   			{Reference, Node} ! {data, [{Hashcode, {MapKey, MapValue}}]}
				   			end;
				   		_ ->
				   			%skicka till den piden
				   			% io:format("3  HASH: ~p Node: ~p MK: ~p MV: ~p ~n", [Reference, Node, MapKey, MapValue]),

				   			{Reference, Node} ! {data, [{Hashcode, {MapKey, MapValue}}]}
				   	end
		       	end, Map),

		       % io:format("MASTER 11: ~p ~n", [{Master, node()}]),
		       case {Master, node()} of
		       		{{MasterPid, MasterNode}, ThisNode} when ThisNode =:= MasterNode ->
		       				       			io:format("                 MASTERSame: ~p CURRENT: ~p ~n", [MasterNode, ThisNode]),

						MasterPid ! {reducer_pids, {self(), Ref, [{Hashcode, lists:nth(erlang:phash(Hashcode, length(Nodes)), Nodes)} || {Hashcode, {MapKey, _MapValue}}<-Map]}};
		       		{{MasterPid, MasterNode}, ThisNode} ->
		       			% io:format("                 MASTER: ~p ThisNode: ~p ~n", [MasterNode, ThisNode]),
	
						{master, MasterNode} ! {reducer_pids, {self(), Ref, [{Hashcode, lists:nth(erlang:phash(Hashcode, length(Nodes)), Nodes)} || {Hashcode, {MapKey, _MapValue}}<-Map]}}
				end


	       end).


% mapper_helper([{Hashcode, {MapKey, MapValue}}|Tail], Nodes, Master, Ref, Reducer) ->
%  	Reference = list_to_atom(integer_to_list(Hashcode)),

%    case whereis(Reference) of
%    		undefined ->
%    			%skapa process, registrera 
%    			ReducerPid = spawn_reducer(lists:nth(erlang:phash(MapKey, length(Nodes)), Nodes), Master, Ref, Reducer),
%    			register(Reference, ReducerPid),
%    			Reference ! {data, [{Hashcode, {MapKey, MapValue}}]};
%    		_ ->
%    			%skicka till den piden
%    			Reference ! {data, [{Hashcode, {MapKey, MapValue}}]}
%    	end


%reduceraren kan inte börja arbeta fören den fått alla nycklar
spawn_reducer(Node, Master, Ref, Reducer, Reference) ->
    spawn_link(Node, fun () ->
    			io:format("REGISTER!!!! ~p ~n", [Reference]),

    	    	register(Reference, self()),

    			% io:format("REDUCING NOW!!!! ~p ~n", [Reference]),
    			Chunk = reducer_loop([]),
    			% TODO: se till att den verkligen stannar här och väntar tills loopen är klar

    			% io:format("after reducer loop ~n"),

		       %% Group the values for each key and apply
		       %% The reducer to each K, value list pair
		       Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(lists:flatten(Chunk))),
				       KV <- Reducer(K,Vs)],

		    	% io:format("REDUCE: ~p ~n", [Reduce]),



		       	case {Master, node()} of
		       		{{MasterPid, MasterNode}, ThisNode} when ThisNode =:= MasterNode ->
		       		   io:format("TO MASTER a: ~p ~n", [{reduce, {{Reference, Node}, Ref, Reduce}}]),

						MasterPid ! {reduce, {{Reference, Node}, Ref, Reduce}};
		       		{{MasterPid, MasterNode}, ThisNode} ->	
		       		       		   io:format("TO MASTER B: ~p ~n", [{reduce, {{Reference, Node}, Ref, Reduce}}]),

						{master, MasterNode} ! {reduce, {{Reference, Node}, Ref, Reduce}}
				end

	       end).


test_distributed() ->
    Mapper = fun (_Key, Text) ->
		     [{Word, 1} || Word <- Text]
	     end,
    Reducer = fun (Word, Counts) ->
		      [{Word, lists:sum(Counts)}]
	      end,
    mapreduce([node()|nodes()], Mapper, 2, Reducer, 10, [{a, ["hello", "world", "hello", "text"]}, {b, ["world", "a", "b", "text"]}]).











