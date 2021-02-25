-module(mapreduce).
-export([mapreduce/5, mapreduce/6, test_distributed/0]).	      

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

    [Pid ! {done, Reducers} || Pid <- UniqueReducerPids],

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

    
spawn_reducer(Master, Ref, Reducer) ->
    spawn_link(fun () ->
    			Chunk = reducer_loop([]),
		       %% Group the values for each key and apply
		       %% The reducer to each K, value list pair
		       Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(lists:flatten(Chunk))),
				       KV <- Reducer(K,Vs)],

		       Master ! {reduce, {self(), Ref, Reduce}}

	       end).




reducer_loop(Data) ->
	receive
		{data, IncomingData} ->
			reducer_loop(Data ++ IncomingData);
		{done, Reducers} ->
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
    ReducerPids = [receive
		      {reducer_pids, {Pid, Ref, RedPid}} ->
			  		RedPid
		  end || Pid <- MapperPids],

    [{list_to_atom(integer_to_list(Pid)), Node} ! {done, Reducers} || {Pid, Node} <- lists:sort(remove_duplicates(lists:flatten(ReducerPids)))],
    

    ReducerPidsAndNodes = [{list_to_atom(integer_to_list(Pid)), Node} || {Pid, Node} <- lists:sort(remove_duplicates(lists:flatten(ReducerPids)))],


    Output = [receive 
		  {reduce, {P, Data}} ->
		      Data
	      end || P<- ReducerPidsAndNodes],

    %% Flatten the output from the reducers
    %% sort becouse it looks nice :-)
    lists:sort(lists:flatten(Output)).

spawn_mapper(CurrentNode, Nodes, Master, Ref, Mapper, Reducer, Reducers, Data) ->
    spawn_link(lists:nth(CurrentNode, Nodes), fun () ->
		       %% phash: hash erlang term to `Reducers` bins.
		       %% In this case we tag each value with the same
		       %% value so that they will be processed by the
		       %% same reducer.
		       	ets:new(keys, [set, private, named_table]),

		       Map = [{erlang:phash(MapKey, Reducers), {MapKey, MapValue}} ||
				 %% For each element in Data
				 {DataKey, DataValue} <- Data, 
				 %% Apply the Mapper to the key and value
				 %% and iterate over those
				 {MapKey, MapValue} <- Mapper(DataKey, DataValue)],

		       lists:foreach(fun({Hashcode, {MapKey, MapValue}}) ->

				 	Reference = list_to_atom(integer_to_list(Hashcode)),
				 	Node = lists:nth(erlang:phash(Hashcode, length(Nodes)), Nodes),


				   case rpc:call(Node, erlang, whereis, [Reference]) of
				   		undefined ->

				   			case ets:lookup(keys, Reference) of
				   				[] ->
				   					ets:insert(keys, {Reference, ok}),
						   			ReducerPid = spawn_reducer(Node, Master, Ref, Reducer, Reference),
						   			if
						   				Node =:= node() ->
						   					ReducerPid ! {data, [{Hashcode, {MapKey, MapValue}}]};
						   				true ->
								   			{Reference, Node} ! {data, [{Hashcode, {MapKey, MapValue}}]}
						   			end;
						   		[{_, ok}] ->
						   			{Reference, Node} ! {data, [{Hashcode, {MapKey, MapValue}}]}
						   	end;
				   		_ ->
				   			{Reference, Node} ! {data, [{Hashcode, {MapKey, MapValue}}]}
				   	end
		       	end, Map),

		       case {Master, node()} of
		       		{{MasterPid, MasterNode}, ThisNode} when ThisNode =:= MasterNode ->
						MasterPid ! {reducer_pids, {self(), Ref, [{Hashcode, lists:nth(erlang:phash(Hashcode, length(Nodes)), Nodes)} || {Hashcode, {_MapKey, _MapValue}}<-Map]}};
		       		{{_MasterPid, MasterNode}, _ThisNode} ->	
						{master, MasterNode} ! {reducer_pids, {self(), Ref, [{Hashcode, lists:nth(erlang:phash(Hashcode, length(Nodes)), Nodes)} || {Hashcode, {_MapKey, _MapValue}}<-Map]}}
				end,

				ets:delete(keys)


	       end).

spawn_reducer(Node, Master, _Ref, Reducer, Reference) ->
	case rpc:call(Node, erlang, whereis, [Reference]) of 
		undefined ->
		   	spawn_link(Node, fun () ->
    			register(Reference, self()),
    			Chunk = reducer_loop([]),

		       %% Group the values for each key and apply
		       %% The reducer to each K, value list pair
		       Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(lists:flatten(Chunk))),
				       KV <- Reducer(K,Vs)],

		       	case {Master, node()} of
		       		{{MasterPid, MasterNode}, ThisNode} when ThisNode =:= MasterNode ->
						MasterPid ! {reduce, {{Reference, Node}, Reduce}};
		       		{{_MasterPid, MasterNode}, _ThisNode} ->	
						{master, MasterNode} ! {reduce, {{Reference, Node}, Reduce}}
				end

	       end);
		Pid -> Pid
	end.

test_distributed() ->
    Mapper = fun (_Key, Text) ->
		     [{Word, 1} || Word <- Text]
	     end,
    Reducer = fun (Word, Counts) ->
		      [{Word, lists:sum(Counts)}]
	      end,
    mapreduce([node()|nodes()], Mapper, 2, Reducer, 10, [{a, ["hello", "world", "hello", "text"]}, {b, ["world", "a", "b", "text"]}]).






