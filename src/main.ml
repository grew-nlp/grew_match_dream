open Dream_utils
open Gmd_main

let ping_route = 
  Dream.post "ping" (fun _ -> Dream.html ~headers:["Content-Type", "text/plain"] "{}")

let all_routes = [
  ping_route;
]

let _ =
  try
    let required = ["port"] in
    Dream_config.load ~required ();
    Log.init ?prefix:(Dream_config.get_string_opt "prefix") ();
    let _ = load_data () in
    Dream.run
    ~error_handler:Dream.debug_error_handler
    ~port: (Dream_config.get_int "port")
    @@ Dream.logger
    @@ Dream.router all_routes
  with Error msg -> 
    stop "%s" (Yojson.Basic.pretty_to_string msg)
