open Dream_utils

let ping_route = 
  Dream.post "ping" (fun _ -> Dream.html ~headers:["Content-Type", "text/plain"] "{}")

let all_routes = [
  ping_route;
]

let _ =
  let required = ["port"] in
  Dream_config.load ~required ();
  Log.init ?prefix:(Dream_config.get_string_opt "prefix") ();
  (* let _ = 
    try Unix.mkdir (Dream_config.get_string "storage") 0o755
    with Unix.Unix_error(Unix.EEXIST, _, _) -> Ags_main.load_from_storage () in *)

  Dream.run
    ~error_handler:Dream.debug_error_handler
    ~port: (Dream_config.get_int "port")
  @@ Dream.logger
  @@ Dream.router all_routes
