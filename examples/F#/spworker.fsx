(*
  Simple Pirate worker
  Connects REQ socket to tcp://*:5556
  Implements worker part of LRU queueing
*)
#r @"bin/fszmq.dll"
#r @"bin/fszmq.devices.dll"
open fszmq
open fszmq.Context
open fszmq.devices
open fszmq.Polling
open fszmq.Socket

#load "zhelpers.fs"

let LRU_READY = encode "\u001"

let rand = srandom()

let main () =
  use ctx = new Context(1)
  use worker = req ctx
  
  // set random identity to make tracing easier
  let identity = sprintf "%04X-%04X" (rand.Next 0x10000) (rand.Next 0x10000)
  (ZMQ.IDENTITY,encode identity) |> set worker
  connect worker "tcp://localhost:5556"

  // tell broker we're ready for work
  printfn "I: (%s) worker ready" identity
  send worker LRU_READY

  let rec loop cycles =
    let msg = recvAll worker
    let continue' n = 
      sleep n // do some heavy work
      sendAll worker msg
      loop (cycles + 1)

    // simulate various problems, after a few cycles
    match cycles,(rand.Next 5) with
    | n,0 when n > 3  ->  printfn "I: (%s) simulating a crash" identity
                          // interrupted!
    | n,3 when n > 3  ->  printfn "I: (%s) simulating CPU overload" identity
                          continue' 3 
    | _               ->  printfn "I: (%s) normal reply" identity
                          continue' 1
  loop 1

  EXIT_SUCCESS

main()
