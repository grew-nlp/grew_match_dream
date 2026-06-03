open Dream_utils
open Gmd_utils
open Gmd_types
open Gmd_main
open Grewlib

let ping_route =
  Dream.post "ping" (fun _ -> Dream.html ~headers:["Content-Type", "text/plain"] "{}")

let refresh_all_route =
  Dream.post "refresh_all"
    (fun request ->
      match%lwt Dream.body request with
      | "" ->
        let json = wrap (fun () -> Table.refresh_all (); `Null) () in
        Log.info "<refresh_all> ==> %s" (report_status json);
        reply json
      | _ -> Dream.empty `Bad_Request
    )

let reload_route =
  Dream.post "reload"
    (fun request ->
      match%lwt Dream.body request with
      | "" ->
        let json = wrap (fun () -> load_data (); `Null) () in
        Log.info "<reload> ==> %s" (report_status json);
        reply json
      | _ -> Dream.empty `Bad_Request
    )

let refresh_corpus_route =
  Dream.post "refresh_corpus"
    (fun request ->
      match%lwt Dream.form ~csrf:false request with
      | `Ok ["corpus_id", corpus_id] ->
        let json = wrap (fun () -> Table.refresh_one corpus_id; `Null) () in
        Log.info "<refresh_corpus> corpus_id=[%s] ==> %s" corpus_id (report_status json);
        reply json
      | _ -> Dream.empty `Bad_Request
    )

let build_generic_route (service_name, service_fct, full_log) =
  (* let open Yojson.Basic.Util in *)
  Dream.post service_name
    (fun request ->
      let%lwt body = Dream.body request in
      let param = body |> Yojson.Basic.from_string in
      let json = wrap 
      (fun () ->
        service_fct param
      ) () in
      if full_log
        then Log.info "<%s> param=%s ==> %s" service_name (Yojson.Basic.to_string param) (report_status json)
        else Log.info "<%s> ==> %s" service_name (report_status json);
      reply json
    )

let build_option service_name =
  Dream.options service_name (fun _req ->
    Dream.respond ~headers:[ ("Allow", "OPTIONS, GET, HEAD, POST") ] ""
  )

let cors_middleware handler req =
      let handlers =
        [ "Allow", "OPTIONS, GET, HEAD, POST"
        ; "Access-Control-Allow-Origin", "*"
        ; "Access-Control-Allow-Methods", "OPTIONS, GET, HEAD, POST"
        ; "Access-Control-Allow-Headers", "Content-Type"
        ; "Access-Control-Max-Age", "86400"
        ]
      in
      let%lwt res = handler req in
      handlers
      |> List.map (fun (key, value) -> Dream.add_header res key value)
      |> ignore;
      Lwt.return res


let new_corpus_route =
  Dream.post "new_corpus"
    (fun request ->
      match Dream_config.get_string_opt "upload" with
      | None -> wrap (fun () -> error "Missing `upload` in config") () |> reply
      | Some upload ->
        let session_id = Printf.sprintf "%04x%04x%04x%04x" (Random.int 0xFFFF) (Random.int 0xFFFF) (Random.int 0xFFFF) (Random.int 0xFFFF) in
        let upload_dir = Filename.concat upload session_id in
        FileUtil.mkdir ~parent:true upload_dir;
        match%lwt stream_request ~upload_dir request with
        | (param_map,_) ->
          let json = wrap
          (fun () -> 
            let (config, snippets) =
            match String_map.find_opt "schema" param_map with 
            | Some "UD" -> ("ud", "ud")
            | Some "SUD" -> ("sud", "sud")
            | Some "Parseme" -> ("ud", "parseme") 
            | Some s -> error "Unknown schema `%s`" s
            | None -> error "No schema given" in
            let corpusbank = Dream_config.get_string "corpusbank" in
            let corpus_desc = [
              Some ("id", `String session_id);
              Some ("config", `String config);
              String_map.find_opt "name" param_map |> CCOption.map (fun v -> ("name", `String v));
              Some ("snippets", `String snippets);
              Some ("directory", `String upload_dir);
            ] 
            |> CCList.filter_map CCFun.id
            |> (fun x -> `Assoc x)
          
          in
            Corpus_desc.compile (Corpus_desc.of_json corpus_desc);
            let desc_file = concat_filenames [upload_dir; "_build_grew"; session_id; "desc.json"] in
            let desc = Yojson.Basic.from_file desc_file in
            Yojson.Basic.to_file
              (Filename.concat corpusbank (session_id ^ ".json"))
              (`List [corpus_desc]);
            load_data();
            `Assoc [
              "session_id", `String session_id;
              "desc", desc
            ]
          ) () in
          reply json
    )

let static_route =
  Dream.get "/**" (Dream.static "static")

let basic_routes = [
  ping_route;
  static_route;
  refresh_all_route;
  reload_route;
  refresh_corpus_route;
  new_corpus_route
]

let all_routes =
  List.fold_left
    (fun acc (service_name, service_fct, full_log) ->
      (build_option service_name) :: (build_generic_route (service_name, service_fct, full_log)) :: acc
    ) basic_routes [
      ("get_build_file", get_build_file, true);
      ("more_results", more_results, true);
      ("search", search, true);
      ("search_multi", search_multi, true);
      ("get_corpora_desc", get_corpora_desc, false);
      ("get_single", get_single, false);
      ("conll", conll, true);
      ("count", count, true);
      ("count_multi", count_multi, true);
      ("save", save, true);
      ("tsv_export", tsv_export, true);
      ("conll_export", conll_export, true);
      ("parallel", parallel, true);
      ("download_tgz", download_tgz, true);
    ]

let _ =
  try
    let required = ["port"] in
    Dream_config.load ~required ();
    Log.init ?prefix:(Dream_config.get_string_opt "prefix") ();
    let _ = Random.self_init () in
    let _ = load_data () in
    let _ = refresh () in
    Dream.run
    ~error_handler:Dream.debug_error_handler
    ~port: (Dream_config.get_int "port")
    @@ cors_middleware
    @@ Dream.logger
    @@ Dream.router all_routes
  with Error msg -> 
    stop "%s" (Yojson.Basic.pretty_to_string msg)
