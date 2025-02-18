open Printf

open Dep2pictlib
open Grewlib
open Conll

open Dream_utils
open Gmd_global
open Gmd_utils
open Gmd_types


let reduce_arr arr = 
  if Array.length arr <= Global.max_grid_size
  then arr
  else
    let new_arr = Array.sub arr 0 (Global.folded_size+1) in
    let merge_size = ref 0 in
    Array.iteri (fun index (_,size) -> if index >= Global.folded_size then merge_size := !merge_size + size) arr;
    new_arr.(Global.folded_size) <- (Some "__*__", !merge_size);
    new_arr

let filter arr =
  if Array.length arr > Global.max_grid_size
  then (fun x -> Array_.find arr (fun (e,_) -> e=x) < Global.folded_size)
  else (fun _ -> true)


(* ============================================================================================================================ *)
let search param =
  let start_time = Unix.gettimeofday () in

  let display = get_attr "display" param in

  let _ = Draw_config.update display in

  let uuid = php_uniqid () in
  let _ = Unix.mkdir (datadir uuid) 0o755 in

  let corpus_id = get_string_attr "corpus_id" param in
  let (_, corpus, corpus_desc) = Table.get_corpus corpus_id in
  let config = Corpus_desc.get_config corpus_desc in
  let request = Request.parse ~config (get_string_attr "request" param) in

  let clust_item_list =
    get_attr "clust" param
    |> get_clust_item_list
    |> (List.map (Request.parse_cluster_item ~config request)) in

  let (clusters_list, status, ratio) =
    Corpus.bounded_search
      ~config ~ordering:(get_string_attr_opt "order" display) (Some Global.max_results) (Some Global.timeout_search) []
      (fun graph_index sent_id _ pos_in_graph nb_in_graph matching x -> 
        {Session.graph_index; pos_in_graph; nb_in_graph; sent_id; matching}:: x)
        request clust_item_list corpus in

  let full_clusters = Clustered.map (fun l -> { Session.data = Array.of_list (List.rev l); next = 0}) clusters_list in
  let sizes = Clustered.sizes (fun x -> Array.length x.Session.data) full_clusters in
  
  let (json_clusters, clusters) = 
    match sizes with 
  | [] -> (("cluster_single", `Null), full_clusters)
  | [som_size] ->
    let arr = Array.of_list (String_opt_map.fold (fun k v acc -> (k,v) :: acc) som_size []) in
    cluster_sort arr;
    (("cluster_array", json_values_sizes arr), full_clusters)
  | [som_size1; som_size2] ->
    let arr_1 = Array.of_list (String_opt_map.fold (fun k v acc -> (k,v) :: acc) som_size1 []) in
    let arr_2 = Array.of_list (String_opt_map.fold (fun k v acc -> (k,v) :: acc) som_size2 []) in
    Array.sort (fun (_,s1) (_,s2) -> compare s2 s1) arr_1; (* build an array in reverse size order *)
    Array.sort (fun (_,s1) (_,s2) -> compare s2 s1) arr_2; (* build an array in reverse size order *)

    let folded_arr_1 = reduce_arr arr_1
    and folded_arr_2 = reduce_arr arr_2
    and folded_clusters = 
      Clustered.merge_keys 
        (Some "__*__")
        Session.append_cluster
        Session.empty_cluster 
        [filter arr_1; filter arr_2] 
        full_clusters in

  (
    ("cluster_grid",
    `Assoc [
      ("rows", json_values_sizes folded_arr_1); 
      ("columns", json_values_sizes folded_arr_2);
      ("total_rows_nb", `Int (Array.length arr_1));
      ("total_columns_nb", `Int (Array.length arr_2));
      ("cells", `List (
        Array.fold_right
          (fun (k1,_) acc_1 -> 
            `List (
              Array.fold_right
                (fun (k2,_) acc_2 ->
                  let cluster = Clustered.get_opt Session.empty_cluster [k1; k2] folded_clusters in
                  (`Int (Array.length cluster.Session.data)) :: acc_2
              ) folded_arr_2 []
            ) :: acc_1
          ) folded_arr_1 []
        )
      )
    ]
    ), 
    folded_clusters
  )
  | _ -> failwith "Dim > 2 not handled" in

  let session = {
    Session.corpus_id;
    request;
    last=Unix.gettimeofday ();
    clusters;
  } in
  Client_map.t := String_map.add uuid session !Client_map.t;
  
  let json_output = `Assoc [
      ("uuid", `String uuid);
      ("status", `String status);
      ("ratio", `Float ratio);
      ("nb_solutions", `Int (Clustered.cardinal (fun x -> Array.length x.Session.data) clusters));
      ("pivots", Request.json_bound_names request |> Yojson.Basic.Util.to_assoc |> List.assoc "nodes");
      ("time", `Float (Unix.gettimeofday () -. start_time));
      json_clusters
    ] in
  json_output

(* ============================================================================================================================ *)
let count param =
  let start_time = Unix.gettimeofday () in

  let corpus_id = get_string_attr "corpus_id" param in
  let (_, corpus, corpus_desc) = Table.get_corpus corpus_id in
  let config = Corpus_desc.get_config corpus_desc in
  let string_request = get_string_attr "request" param in
  let request = Request.parse ~config string_request in

  let clust_item_list =
    get_attr "clust" param
    |> get_clust_item_list
    |> (List.map (Request.parse_cluster_item ~config request)) in

  let full_clusters = Corpus.search ~config 0 (fun _ _ _ x -> x+1) request clust_item_list corpus in
  let sizes = Clustered.sizes (fun x -> x) full_clusters in

  let partial_json =
    match sizes with
    | [] -> ("cluster_single", `Null)
    | [som_size] ->
      let arr = Array.of_list (String_opt_map.fold (fun k v acc -> (k,v) :: acc) som_size []) in
      cluster_sort arr;
      ("cluster_array", json_values_sizes arr)
    | [som_size1; som_size2] ->
      let arr_1 = Array.of_list (String_opt_map.fold (fun k v acc -> (k,v) :: acc) som_size1 []) in
      let arr_2 = Array.of_list (String_opt_map.fold (fun k v acc -> (k,v) :: acc) som_size2 []) in
      Array.sort (fun (_,s1) (_,s2) -> compare s2 s1) arr_1; (* build an array in reverse size order *)
      Array.sort (fun (_,s1) (_,s2) -> compare s2 s1) arr_2; (* build an array in reverse size order *)

      let folded_arr_1 = reduce_arr arr_1
      and folded_arr_2 = reduce_arr arr_2
      and folded_clusters = 
        Clustered.merge_keys 
          (Some "__*__")
          (+)
          0 
          [filter arr_1; filter arr_2] 
          full_clusters in

      ("cluster_grid",
      `Assoc [
        ("rows", json_values_sizes folded_arr_1); 
        ("columns", json_values_sizes folded_arr_2);
        ("total_rows_nb", `Int (Array.length arr_1));
        ("total_columns_nb", `Int (Array.length arr_2));
          ("cells", `List (
          Array.fold_right
            (fun (k1,_) acc_1 -> 
              `List (
                Array.fold_right
                  (fun (k2,_) acc_2 ->
                    let cluster = Clustered.get_opt 0 [k1; k2] folded_clusters in
                    (`Int cluster) :: acc_2
                ) folded_arr_2 []
              ) :: acc_1
            ) folded_arr_1 []
          )
        )
      ]
      )

    | _ -> failwith "Dim > 2 not handled" in

  let nb_solutions = Clustered.cardinal (fun x -> x) full_clusters in
  let time = Unix.gettimeofday () -. start_time in 

  ["nb_solutions", `Int nb_solutions; "time", `Float time; partial_json]
  |> (fun l -> if Redundant.record string_request = 0 then ("redundant", `Int Redundant.bound)::l else l)
  |> (fun l -> `Assoc l)

(* ============================================================================================================================ *)
(* produce 10 more solutions *)
let more_results param =
  try
    let uuid = get_string_attr "uuid" param in
    let cluster_path = get_named_path_attr "named_cluster_path" param in
    let session = String_map.find uuid !Client_map.t in
    let (_,corpus,corpus_desc) = Table.get_corpus session.corpus_id in
    let config = Corpus_desc.get_config corpus_desc in
    let audio = Corpus_desc.get_flag "audio" corpus_desc in
    let rtl = Corpus_desc.get_flag "rtl" corpus_desc in
    let cluster = Clustered.get_opt Session.empty_cluster cluster_path session.Session.clusters in
    let start_index = cluster.next in
    let max_index = Array.length (cluster.data) in
    let stop_index = min (start_index + Global.nbre_sol_page) max_index in
    let more_flag = stop_index < max_index in

    let rich_sentence ?deco index =
      try
        let graph = Corpus.get_graph index corpus in
        let sentence_filename = Graph.get_meta_opt "_filename" graph in
        match (audio, Corpus.is_conll corpus) with
          | (true, _) -> (Graph.to_sentence_audio ?deco graph, sentence_filename)
          | (false, true) -> (((match Graph.to_sentence ?deco graph with "" -> Corpus.get_text index corpus | x -> x), None, None), sentence_filename)
          | (false, false) -> ((Corpus.get_text index corpus, None, None), sentence_filename)
      with _ -> (("", None, None), None) in

    let json_list =
      List.map
        (fun current_index ->
          let occ = cluster.data.(current_index) in
          let deco = Matching.build_deco session.request occ.matching in
          let index = occ.graph_index in
          let graph = Corpus.get_graph index corpus in
          let filename = sprintf "%d_%d" occ.graph_index occ.pos_in_graph in
          let list_item =
            match occ.nb_in_graph with
            | 1 -> occ.sent_id
            | len -> sprintf "%s [%d/%d]" occ.sent_id (len - occ.pos_in_graph) len in

            let ((sentence, audio_bounds, sound_url), sentence_filename) = rich_sentence ~deco index in

            (* let sent_with_context =
              if !Draw_config.current.context
              then
                let prev_sent = try (Corpus.get_text (index-1) corpus)^"</br>" with _ -> "" in
                let next_sent = try "</br>"^(Corpus.get_text (index+1) corpus) with _ -> "" in
                sprintf "%s <font color=\"#FC5235\">%s</font> %s" prev_sent sentence next_sent
              else sprintf "<font color=\"#FC5235\">%s</font>" sentence in *)

              let (sent_with_context, extended_audio_bounds) =
              if !Draw_config.current.context
              then
                let (prev_sent, new_left_bound) = match rich_sentence (index-1) with
                | (("",_,_),_) -> (None, None)
                | ((_,_,url),_) when url <> sound_url -> (None, None) (* different sound_url *)
                | (_,sf) when sf <> sentence_filename -> (None, None) (* different filenames *)
                | ((prev_text,prev_bounds,_),_) -> (Some prev_text , CCOption.map fst prev_bounds) in
                let (next_sent, new_right_bound) = match rich_sentence (index+1) with
                | (("",_,_),_) -> (None, None)
                | ((_,_,url),_) when url <> sound_url -> (None, None) (* different sound_url *)
                | (_,sf) when sf <> sentence_filename -> (None, None) (* different filenames *)
                | ((next_text,next_bounds,_),_) -> (Some next_text , CCOption.map snd next_bounds) in
                (sprintf "%s<font color=\"#FC5235\">%s</font>%s"
                  (match prev_sent with None -> "" | Some p -> p^"</br>")
                  sentence
                  (match next_sent with None -> "" | Some p -> "</br>"^p),
                  match audio_bounds with
                  | None -> None
                  | Some (b,e) -> Some (CCOption.get_or ~default:b new_left_bound, CCOption.get_or ~default:e new_right_bound)
                )
              else (sprintf "<font color=\"#FC5235\">%s</font>" sentence, audio_bounds) in

           let meta_list =
              List.filter
                (function
                | ("sent_id",_) | ("sound_url",_) | ("code",_) | ("url",_) | ("_filename", _) -> false
                | (s,_) when CCString.prefix ~pre:"##" s -> false
                | _ -> true
                ) (Graph.get_meta_list graph) in

           let json =
             match Corpus_desc.get_display corpus_desc with
             | None ->
               let audio_info =
                 match (audio, sound_url, extended_audio_bounds) with
                 | (true, Some url, Some (i,f)) -> Some (sprintf "%s#t=%g,%g" url i f)
                 | (true, Some url, None) -> Some (sprintf "%s" url)
                 | _ -> None in
               save_dep uuid ~config ?audio_info rtl filename list_item deco graph sent_with_context meta_list
             | Some -1 -> save_dot uuid ~config filename list_item deco graph sent_with_context meta_list
             | Some i ->
               let subgraph = Matching.subgraph graph occ.matching i in
               save_dot uuid ~config filename list_item deco subgraph sent_with_context meta_list in
           json
        ) (CCList.range' start_index stop_index) in

    Client_map.t :=
      String_map.add uuid
        { session with 
          last=Unix.gettimeofday ();
          clusters = Clustered.update (fun _ -> { cluster with next=stop_index}) cluster_path Session.empty_cluster session.clusters ;
        }
        !Client_map.t;
    `Assoc [("more", `Bool more_flag); ("items", `List json_list)]
  with Not_found -> raise (Error (`Assoc [("message", `String "more_results service: not connected")]))

(* ============================================================================================================================ *)
let export param = 
  try
    let uuid = get_string_attr "uuid" param in
    let pivot = get_string_attr "pivot" param in
    let session = String_map.find uuid !Client_map.t in
    let (_,corpus,_) = Table.get_corpus session.corpus_id in
    let filename = Filename.concat (datadir uuid) "export.tsv" in
    let out_ch = open_out filename in
    begin
      match pivot with
      | "" -> fprintf out_ch "sent_id\tsentence\n"
      | _ -> fprintf out_ch "sent_id\tleft_context\tpivot\tright_context\n"
    end;

    let export_cluster cluster =
      Array.iter (fun {Session.graph_index; sent_id; matching; _} ->
          let graph = Corpus.get_graph graph_index corpus in
          let sentence =
            match pivot with
            | "" -> Graph.to_sentence graph
            | pivot ->
              let deco = Matching.build_deco session.request matching in
              Graph.to_sentence ~pivot ~deco graph
              |> (Str.global_replace (Str.regexp_string "<span class=\"highlight\">") "\t")
              |> (Str.global_replace (Str.regexp_string "</span>") "\t") in
          fprintf out_ch "%s\t%s\n" sent_id sentence
        ) cluster.Session.data in

    Clustered.iter
    (fun _ cluster ->
      export_cluster cluster
    ) session.clusters;

    close_out out_ch;
    `Null
  with Not_found -> raise (Error (`Assoc [("message", `String "export service: not connected")]))

(* ============================================================================================================================ *)
let conll_export param =
  try
    let uuid = get_string_attr "uuid" param in
    let session = String_map.find uuid !Client_map.t in
    let (_,corpus,corpus_desc) = Table.get_corpus session.corpus_id in
    let config = Corpus_desc.get_config corpus_desc in
    let columns = Corpus.get_columns_opt corpus in

    let filename = Filename.concat (datadir uuid) "export.conllu" in
    let out_ch = open_out filename in

    begin
      match columns with
      | Some c -> fprintf out_ch "%s\n" (Conll_columns.to_string c)
      | None -> ()
    end; 

    begin
      match Clustered.depth session.clusters with 
      | 0 -> (* No clustering --> keep the sent_id order  *)
        let cluster = Clustered.get_opt Session.empty_cluster [] session.clusters in
        let current_sent_id = ref "" in
        Array.iter
          (fun occ ->
            if occ.Session.sent_id <> !current_sent_id
            then
              begin
                let gr = Corpus.get_graph occ.Session.graph_index corpus in
                fprintf out_ch "%s\n" (gr |> Graph.to_json |> Conll.of_json |> (Conll.to_string ?columns ~config));
                current_sent_id := occ.Session.sent_id
              end
          ) cluster.Session.data
      | _ -> (* clustered results: collect all graph_indexes *)
        let graph_index_set =
          Clustered.fold
          (fun _ cluster acc ->
            Array.fold_left 
              (fun acc2 occ -> Int_set.add occ.Session.graph_index acc2) 
              acc cluster.Session.data
          ) session.clusters Int_set.empty in
        Int_set.iter
          (fun graph_index ->
            let gr = Corpus.get_graph graph_index corpus in
            fprintf out_ch "%s\n"  (gr |> Graph.to_json |> Conll.of_json |> (Conll.to_string ?columns ~config))
          ) graph_index_set
    end;

    close_out out_ch;
    `Null
  with Not_found -> raise (Error (`Assoc [("message", `String "conll_export service: not connected")]))

(* ============================================================================================================================ *)
let conll param =
  try
    match get_string_attr_opt "uuid" param with
    | Some uuid ->
      let current_view = get_int_attr "current_view" param in
      let cluster_path = get_named_path_attr "named_cluster_path" param in
      let session = String_map.find uuid !Client_map.t in
      let (_,corpus,corpus_desc) = Table.get_corpus session.corpus_id in
      let config = Corpus_desc.get_config corpus_desc in
      let cluster = Clustered.get_opt Session.empty_cluster cluster_path session.Session.clusters in
      let occ = cluster.Session.data.(current_view) in
      let columns = Corpus.get_columns_opt corpus in
      let gr = Corpus.get_graph occ.Session.graph_index corpus in
      `String (gr |> Graph.to_json |> Conll.of_json |> (Conll.to_string ?columns ~config))
    | None -> (* get CoNLL before calling grew_web *)
      let corpus_id = get_string_attr "corpus_id" param in
      let sent_id = get_string_attr "sent_id" param in
      let (_,corpus,corpus_desc) = Table.get_corpus corpus_id in
      let columns = Corpus.get_columns_opt corpus in
      let config = Corpus_desc.get_config corpus_desc in
      match Corpus.graph_of_sent_id sent_id corpus with
      | None -> raise (Error (`Assoc [("message", `String "Unknown sent_id"); ("sent_id", `String sent_id)]))
      | Some gr ->
        `String (gr |> Graph.to_json |> Conll.of_json |> (Conll.to_string ?columns ~config))

  with Not_found -> raise (Error (`Assoc [("message", `String "conll service: not connected")]))

(* ============================================================================================================================ *)
let parallel param = 
  try
    let uuid = get_string_attr "uuid" param in
    let corpus_id = get_string_attr "corpus_id" param in
    let sent_id = get_string_attr "sent_id" param in

    let (_,corpus,corpus_desc) = Table.get_corpus corpus_id in
    let config = Corpus_desc.get_config corpus_desc in
    match Corpus.graph_of_sent_id sent_id corpus with
    | None -> raise (Error (`Assoc [("message", `String "Unknown sent_id"); ("sent_id", `String sent_id)]))
    | Some graph ->
      match Corpus.is_conll corpus with
      | true ->
        let dep = Graph.to_dep ~filter: Draw_config.filter ~config graph in
        let d2p =
          try Dep2pictlib.from_dep ~rtl:(Corpus_desc.get_flag "rtl" corpus_desc) dep
          with Dep2pictlib.Error json -> raise (Error (`Assoc [("message", `String "Dep2pict error"); ("sent_id", `String sent_id); ("json", json)])) in
        let filename = sprintf "%04x%04x.svg" (Random.int 0xFFFF) (Random.int 0xFFFF) in
        let _ = Dep2pictlib.save_svg ~filename:(Filename.concat (datadir uuid) filename) d2p in
        `String filename
      | false ->
        let dot = Graph.to_dot ~config graph in
        let temp_file_name,out_ch = Filename.open_temp_file ~mode:[Open_rdonly;Open_wronly;Open_text] "grew_" ".dot" in
        fprintf out_ch "%s" dot;
        close_out out_ch;
        let filename = sprintf "%04x%04x.svg" (Random.int 0xFFFF) (Random.int 0xFFFF) in
        ignore (Sys.command(sprintf "dot -Tsvg -o %s %s " (Filename.concat (datadir uuid) filename) temp_file_name));
        `String filename
  with Not_found -> raise (Error (`Assoc [("message", `String "parallel service: not connected")]))

(* ============================================================================================================================ *)
let save param =
  try
    let uuid = get_string_attr "uuid" param in
    let folder = Filename.concat (Dream_config.get_string "storage") "shorten" in
    let filename = Filename.concat folder (uuid ^ ".json") in
    Yojson.Basic.to_file filename param;
    `Null
  with Not_found -> raise (Error (`Assoc [("message", `String "save service: not connected")]))

(* ============================================================================================================================ *)
let get_corpora_desc param =
  let open Yojson.Basic.Util in
  let groups = param |> member "instance_desc" |> to_list in
  let new_groups =
    List.map 
    (fun group -> 
      let corpora_list = group |> member "corpora" |> to_list |> List.map to_string in
      let new_corpora_list =
        List.map
        (fun corpus_id ->
          begin
            match String_map.find_opt corpus_id !Global.corpora_map with
            | Some (corpus_desc, _) -> Corpus_desc.to_json corpus_desc
            | None -> `Assoc [("id", `String corpus_id); ("error", `String "No description found")]
          end
        ) corpora_list in
        let new_group = ("corpora", `List new_corpora_list) :: (List.remove_assoc "corpora" (group |> to_assoc)) in
        `Assoc new_group
    ) groups in
    `List new_groups

(* ============================================================================================================================ *)
let get_build_file param =
  let corpus_id = get_string_attr "corpus_id" param in
  let file = get_string_attr "file" param in
  let (corpus_desc, _) = String_map.find corpus_id !Global.corpora_map in 
  let directory = Corpus_desc.get_directory corpus_desc in
  let full_file = Filename.concat directory (Filename.concat "_build_grew" (Filename.concat corpus_id file)) in
  let data = CCIO.(with_in full_file read_all) in
    `String data

(* ============================================================================================================================ *)
let dowload_tgz param =
  let corpus_id = get_string_attr "corpus_id" param in
  let tgz_file = sprintf "%s.tgz" corpus_id in
  let (corpus_desc, _) = String_map.find corpus_id !Global.corpora_map in 
  let directory = Corpus_desc.get_directory corpus_desc in
  let full_file = concat_filenames [directory; "_build_grew"; corpus_id; tgz_file] in
  let dst = concat_filenames [Dream_config.get_string "storage"; "tgz"; tgz_file] in
  let _ = try Unix.unlink dst with _ -> () in
  let _ = Unix.symlink ~to_dir:false full_file dst in
    `String (concat_filenames ["tgz"; tgz_file])


(* ================================================================================ *)
(* main *)
(* ================================================================================ *)

(* Define and start a recurrent call to [Client_map.purge] *)
let rec refresh () =
  Client_map.purge ();
  Lwt_timeout.start (Lwt_timeout.create Global.refresh_frequency (fun () -> refresh()))


let load_data () = 
  let corpusbank = Dream_config.get_string "corpusbank" in
  let map = load_corpusbank corpusbank in
  Global.corpora_map := map;
  Printf.printf "==> %d corpus descriptions found in `%s`\n%!" (String_map.cardinal map) corpusbank;
  Table.init (String_map.cardinal map)
