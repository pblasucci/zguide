(*
  Paranoid Pirate queue
*)
#r @"bin/fszmq.dll"
#r @"bin/fszmq.devices.dll"
open fszmq
open fszmq.Context
open fszmq.devices
open fszmq.Polling
open fszmq.Socket

#load "zhelpers.fs"

type date = System.DateTime

open System.Collections.Generic

let [<Literal>] HEARTBEAT_LIVENESS =    3.0  // 3-5 is reasonable
let [<Literal>] HEARTBEAT_INTERVAL = 1000.0 // microseconds

// Paranoid Pirate Protocol constants
let PPP_READY     = encode "\u001"  // signals worker is ready
let PPP_HEARTBEAT = encode "\u002"  // signals worker heartbeat

// this defines one active worker in our worker list
type Worker =
  { Address   : byte array  // address of worker
    Identity  : string      // printable identity
    Expiry    : date        // expires at this time
  } with
      // construct new worker
      static member New address = 
        { Address   = address 
          Identity  = decode address 
          Expiry    = date.Now.AddMilliseconds( HEARTBEAT_INTERVAL 
                                              * HEARTBEAT_LIVENESS ) }

// worker is ready, remove if on list and move to end
let workerReady self (workers:ResizeArray<_>) =
  let index = workers.FindIndex(fun {Identity=i} -> self.Identity = i)
  if index <> -1 then workers.RemoveAt(index)
  workers.Add(self)

// return next available worker address
let nextWorker (workers:ResizeArray<_>) =
  match workers.Count with
  | 0 ->  None
  | n ->  let {Address=a} = workers.[0]
          workers.RemoveAt(0)
          Some(a)

// look for & kill expired workers
let purge (workers:ResizeArray<_>) =
  workers.RemoveAll(fun {Expiry=e} -> date.Now > e) |> ignore

let main () =
  use ctx = new Context(1)
  use frontend  = ctx |> route
  use backend   = ctx |> route
  bind frontend "tcp://*:5555"
  bind backend  "tcp://*:5556"

  // list of available workers
  let workers = ResizeArray<_>()
  
  // handle worker activity on backend
  let handleBE backend =
    // use worker address for LRU routing
    let address,msg = let m = recvAll backend in m.[0],m.[1 ..]
    // any sign of life from worker means it's ready
    workerReady (Worker.New address) workers
    // validate control message, or return reply to client
    match msg.Length with
    | 1 ->  if msg <> [| PPP_READY     |]
            && msg <> [| PPP_HEARTBEAT |]
              then  printfn "E: invalid message from worker"
                    dumpMsg msg
    | _ ->  sendAll frontend msg

  // now get next client request, route to next worker
  let handleFE frontend =
    let msg = recvAll frontend
    nextWorker workers
    |> Option.iter (fun address -> msg
                                   |> Array.append [| address |] 
                                   |> sendAll backend)

  let getPollItems () =
    [ yield backend |> pollIn handleBE
      if workers.Count > 0 then yield frontend |> pollIn handleFE ]

  // send out heartbeats at regular intervals
  let pulse heartbeatAt =
    if date.Now >= heartbeatAt 
      then  workers
            |> Seq.map  (fun {Address=a} -> [| a; PPP_HEARTBEAT |])
            |> Seq.iter (sendAll backend)
            date.Now.AddMilliseconds HEARTBEAT_INTERVAL
      else  heartbeatAt
  
  let rec loop heartbeatAt =
    getPollItems() |> poll ((int64 HEARTBEAT_INTERVAL) * 1000L) |> ignore
    purge workers
    loop (pulse heartbeatAt)

  loop (date.Now.AddMilliseconds HEARTBEAT_INTERVAL)
  EXIT_SUCCESS

main()
