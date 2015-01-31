-module(ezkvs_cli).
-export([connect/0, disconnect/1, put/3, delete/2, get/2]).

connect() ->
  {ok, Socket} = gen_tcp:connect("localhost", 2345, [binary, {packet, 4}]),
  Socket.

disconnect(Socket) ->
  gen_tcp:close(Socket).

put(Key, Value, Socket) ->
  ok = gen_tcp:send(Socket, term_to_binary({put, Key, Value})),
  get_reply(Socket).

get(Key, Socket) ->
  ok = gen_tcp:send(Socket, term_to_binary({get, Key})),
  get_reply(Socket).

delete(Key, Socket) ->
  ok = gen_tcp:send(Socket, term_to_binary({delete, Key})),
  get_reply(Socket).
  
get_reply(Socket) ->
  receive 
    {tcp, Socket, Bin} ->
      Val = binary_to_term(Bin),
      io:format("Client result = ~p.~n",[Val])
  end.
