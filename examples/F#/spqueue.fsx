(*
  Simple Pirate queue
  This is identical to the LRU pattern, with no reliability mechanisms
  at all. It depends on the client for recovery. Runs forever.
*)
#r @"bin/fszmq.dll"
#r @"bin/fszmq.devices.dll"
open fszmq
open fszmq.Context
open fszmq.devices
open fszmq.Polling
open fszmq.Socket

#load "zhelpers.fs"

open System.Collections.Generic

let LRU_READY = encode "\u001"

let main () =
  // prepare our context and sockets
  use ctx = new Context(1)
  use frontend  = route ctx
  use backend   = route ctx
  bind frontend "tcp://*:5555"
  bind backend  "tcp://*:5556"

  // queue of available workers
  let workers = Queue<_>()

  let items = 
    [ // handle worker activity on backend
      backend |> pollIn (fun _ -> 
        let msg = recvAll backend
        // use worker address for LRU routing
        msg.[0] |> workers.Enqueue
        // forward message to client,
        // if it's not a READY message
        if msg.[2] <> LRU_READY then msg.[2 ..] |> sendAll frontend) 

      //  Get client request, route to first available worker
      frontend |> pollIn (fun _ -> 
        (recvAll frontend)
        |> Array.append [| workers.Dequeue(); Array.empty |]
        |> sendAll backend) ]
  
  let rec loop okay =
    if okay then
      // poll frontend only if we have available workers
      let items = (if workers.Count = 0 then [items.Head] else items) 
      loop (items |> poll FOREVER)
  loop true

  EXIT_SUCCESS

main()
