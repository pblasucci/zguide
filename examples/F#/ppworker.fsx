(*
  Paranoid Pirate worker
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

let [<Literal>] HEARTBEAT_LIVENESS  =     3   // 3-5 is reasonable
let [<Literal>] HEARTBEAT_INTERVAL  =  1000L  // microseconds
let [<Literal>] INTERVAL_INIT       =  1000   // initial reconnect
let [<Literal>] INTERVAL_MAX        = 32000   // after exponential backoff

// Paranoid Pirate Protocol constants
let PPP_READY     = encode "\u001"  // signals worker is ready
let PPP_HEARTBEAT = encode "\u002"  // signals worker heartbeat

let workerSocket ctx =
  let worker = ctx |> deal
  connect worker "tcp://localhost:5556"

  // tell queue we're ready for work
  printfn "I: worker ready"
  PPP_READY |>> worker

  worker

let main () =
  use ctx = new Context(1)
  let worker = ref (workerSocket ctx)

  let rand = srandom()

  let liveness  = ref HEARTBEAT_LIVENESS
  let interval  = ref INTERVAL_INIT
  let cycles    = ref 0

  let callback socket =
    // get message
    // - 3-part : envelope + content -> request
    // - 1-part : HEARTBEAT          -> heartbeat
    let msg = recvAll socket
    match msg.Length with
    | 3 ->  // simulate various problems, after a few cycles
            incr cycles
            
            if !cycles > 3 && (rand.Next 5) = 0 then
              printf "I: simulating a crash"
              exit -1
            else 
              if !cycles > 3 && (rand.Next 5) = 0 then
                printfn "I: simulating CPU overload"
                sleep 3
            
            printfn "I: normal reply"
            sendAll socket msg
            liveness := HEARTBEAT_LIVENESS
            sleep 1 // do some heavy work

    | 1 ->  if msg.[0] = PPP_HEARTBEAT
              then  liveness := HEARTBEAT_LIVENESS
              else  printfn "E: invalid message"
                    dumpMsg msg

    | _ ->  printfn "E: invalid message"
            dumpMsg msg

    interval := INTERVAL_INIT

  // send out heartbeat to queue if it's time
  let pulse heartbeatAt =
    if date.Now >= heartbeatAt 
      then  printfn "I: worker heartbeat"
            PPP_HEARTBEAT |>> !worker
            date.Now.AddMilliseconds (float HEARTBEAT_INTERVAL)
      else  heartbeatAt

  let rec loop heartbeatAt =
    let items = [ !worker |> pollIn callback ]
    if not (items |> poll (HEARTBEAT_INTERVAL * 1000L)) then
      decr liveness
      if !liveness = 0 then
        printfn "W: heartbeat failure, can't reach queue"
        printfn "W: reconnecting in %d msec..." !interval
        sleep !interval
        if !interval < INTERVAL_MAX then interval := !interval * 2
        (!worker :> System.IDisposable).Dispose()
        worker := workerSocket ctx
        liveness := HEARTBEAT_LIVENESS

    loop (pulse heartbeatAt)

  try
    loop (date.Now.AddMilliseconds (float HEARTBEAT_INTERVAL))
  finally
    (!worker :> System.IDisposable).Dispose()

  EXIT_SUCCESS

main()
