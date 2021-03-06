-module(ezkvs_cli).
-export([connect/0, disconnect/1, put/3, delete/2, get/2, test/0]).
-compile(export_all).

%% MAX_HASH_VALUE = pow(2, 64).
-define(MAX_HASH_VALUE, 2#1111111111111111111111111111111111111111111111111111111111111111).
-define(MAX_BUCKETS, 3).

connect() ->
  Servers = get_data_servers(),
  generate_route(Servers, []).

get_data_servers() ->
  {ok, Socket} = gen_tcp:connect("192.168.1.109", 4345, [binary, {packet, 4}]),
  ok = gen_tcp:send(Socket, term_to_binary({getconfig})),
  Servers = get_reply(Socket),
  gen_tcp:close(Socket),
  Servers.

connect(IPAddr, Port) ->
  io:format("IPAddr = ~p, Port = ~p.~n", [IPAddr, Port]),
  gen_tcp:connect(IPAddr, Port, [binary, {packet, 4}]).

disconnect([{_BucketNum, {_IPAddr, Socket}} | T]) ->
  gen_tcp:close(Socket),
  disconnect(T);
disconnect([]) ->
  ok.

put(Key, Value, RouteTable) ->
  Socket = get_socket(RouteTable, Key),
  ok = gen_tcp:send(Socket, term_to_binary({put, Key, Value})),
  get_reply(Socket).

get(Key, RouteTable) ->
  Socket = get_socket(RouteTable, Key),
  ok = gen_tcp:send(Socket, term_to_binary({get, Key})),
  get_reply(Socket).

delete(Key, RouteTable) ->
  Socket = get_socket(RouteTable, Key),
  ok = gen_tcp:send(Socket, term_to_binary({delete, Key})),
  get_reply(Socket).
  
get_reply(Socket) ->
  receive 
    {tcp, Socket, Bin} ->
      Val = binary_to_term(Bin),
      case Val of 
        {ok, config, Servers} ->
          Servers;
        _Other ->
          io:format("Client result = ~p.~n",[Val])
      end
    after 1000 ->
      io:format("timeout.~n")
  end.

generate_route([{BucketNum, [{IPAddr, Port} | _T]} | T], RouteTable) ->
  io:format("IPAddr = ~p.~n", [IPAddr]),
  {ok, Socket} = connect(IPAddr, Port),
  generate_route(T, [{BucketNum, {IPAddr, Socket}} | RouteTable]);
generate_route([], RouteTable) ->
  RouteTable.

hash(Key) ->
  erlang_murmurhash:murmurhash64b(term_to_binary(Key)).

get_bucket_num(HashValue) ->
  NumberKeysPerBucket = ?MAX_HASH_VALUE div ?MAX_BUCKETS,
  case HashValue rem NumberKeysPerBucket =:= 0 of
    true ->
      HashValue div NumberKeysPerBucket -1;
    false ->
      HashValue div NumberKeysPerBucket
  end.

get_socket(RouteTable, Key) ->
  BucketNum = get_bucket_num(hash(Key)),
  {_BucketNum, {_IPAddr, Socket}}= lists:keyfind(BucketNum, 1, RouteTable),
  Socket.

test() ->
  RouteTable = connect(),
  io:format("RouteTable = ~p.~n", [RouteTable]),
  put(name, "yangmeng", RouteTable),
  get(name, RouteTable),
  %% delete(name, RouteTable),
  get(name, RouteTable),
  put("9191754718kjfjajvkkrjakfda", "yangmeng", RouteTable),
  disconnect(RouteTable).
