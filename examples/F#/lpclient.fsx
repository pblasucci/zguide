(*
  Lazy Pirate client
  Use zmq_poll to do a safe request-reply
  To run, start lpserver and then randomly kill/restart it
*)
#r @"bin/fszmq.dll"
#r @"bin/fszmq.devices.dll"
open fszmq
open fszmq.Context
open fszmq.devices
open fszmq.Polling
open fszmq.Socket

#load "zhelpers.fs"

open System

let [<Literal>] REQUEST_TIMEOUT = 2500L // msecs, (> 1000!)
let [<Literal>] REQUEST_RETRIES =    3  // before we abandon
let [<Literal>] SERVER_ENDPOINT = "tcp://localhost:5555"

let main () =
  use ctx = new Context(1)
  printfn "I: connecting to server..."
  let client = (req >> ref) ctx
  connect !client SERVER_ENDPOINT

  let rec handleReply sequence retriesLeft = 
    let pollSet = 
      [ !client |> pollIn (fun sck ->
          let reply = (recv >> decode) sck
          // we got a reply from the server, must match sequence
          match tryParseInt reply with
          | Some n -> printfn "I: server replied OK (%d)" n
          | None   -> printfn "E: malformed reply from server: %s" reply) ]

    // poll socket for a reply, with timeout
    match pollSet |> poll (REQUEST_TIMEOUT * 1000L) with
    | true  ->  sendRequest (sequence + 1)
    | false ->  match retriesLeft with
                | 0 ->  printfn "E: server seems to be offline, abandoning"
                | n ->  printfn "W: no response from server, retrying"
                        // socket is confused; close it and open a new one
                        (!client :> IDisposable).Dispose()
                        client := req ctx
                        connect !client SERVER_ENDPOINT
                        sequence |> (sprintf "%d" >> encode) |>> !client
                        // send request again, on new socket
                        handleReply sequence (retriesLeft - 1)
  // we send a request, then we work to get a reply
  and sendRequest sequence =
    sequence |> (sprintf "%d" >> encode) |>> !client
    handleReply sequence REQUEST_RETRIES

  try
    sendRequest 1
  finally
    (!client :> IDisposable).Dispose()

  //HACK: why do we need an explicit call to `exit`?!
  exit EXIT_SUCCESS
  
main()
