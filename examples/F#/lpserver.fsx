(*
  Lazy Pirate server
  Binds REQ socket to tcp://*:5555
  Like hwserver except:
   - echoes request as-is
   - randomly runs slowly, or exits to simulate a crash.
*)
#r @"bin/fszmq.dll"
#r @"bin/fszmq.devices.dll"
open fszmq
open fszmq.Context
open fszmq.devices
open fszmq.Polling
open fszmq.Socket

#load "zhelpers.fs"

let main () =
  let rand = srandom()

  use ctx = new Context(1)
  use server = ctx |> rep
  bind server "tcp://*:5555"

  let rec loop cycles =
    let request = server |> recv
    let continue' n = 
      sleep n
      request |>> server
      loop (cycles + 1)

    match cycles,(rand.Next 3) with
    | n,0 when n > 3  ->  printfn "I: simulating a crash"
    | n,1 when n > 3  ->  printfn "I: simulating CPU overload"
                          continue' 2 
    | _               ->  printfn "I: normal request (%s)" (decode request)
                          continue' 1
  
  loop 1
  EXIT_SUCCESS

main()
