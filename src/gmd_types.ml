open Printf

open Grewlib

open Dream_utils
open Gmd_utils
open Gmd_global

(* ==================================================================================================== *)
(* One new [Session] for each execution of [Search] or [Count] *)
module Session = struct
  type occurrence = {
    graph_index: int;     (* graph position in the corpus array *)
    sent_id: string;
    pos_in_graph: int;    (* position of the matching in the ≠ matchings for the same graph *)
    nb_in_graph: int;     (* number of matching in the current graph *)
    matching: Matching.t; (* matching data *)
  }

  type cluster = {
    corpus_id: string;
    corpus_nb: int;
    request: Request.t option;
    data: occurrence array;
    next: int;               (* position of the next solution to produce *)
  }

  let empty_cluster = { corpus_id = ""; corpus_nb = 0; request = None; data=[||]; next=0 }

  let cluster_size { data; _} = Array.length data

  let append_cluster c1 c2 = 
    if c1.next <> 0 || c2.next <> 0
    then failwith "Cannot merge clusters when next is not 0"
    else { c1 with data = Array.append c1.data c2.data }

  type t = {
    last: float;              (* Unix.gettimeofday of the last interaction time *)
    clusters: cluster Clustered.t;
    draw_config: Draw_config.t;
  }
end

(* ==================================================================================================== *)
module Table = struct

  let (t : (Corpus.t * float) option array ref) = ref [||]

  let init size =
    t := Array.make size None

  let refresh () =
    Log.start ();
    let kept = ref 0 in
    let free = ref 0 in
    for i = 0 to (Array.length !t) - 1 do
      match !t.(i) with
      | Some (_,ts) when Unix.gettimeofday () -. ts > Global.timeout_request -> incr free; !t.(i) <- None
      | Some _ -> incr kept;
      | _ -> ()
    done;
    Gc.major ();
    Log.info "[INTERN] Refresh table: |free|=%d |kept|=%d" !free !kept;
    ()

  let get_corpus corpus_id =
    match String_map.find_opt corpus_id !Global.corpora_map with
    | None -> raise (Error (`Assoc [("message", `String "Unknown corpus"); ("corpus_id", `String corpus_id)]))
    | Some (corpus_desc, corpus_index) ->
      match !t.(corpus_index) with
      | Some (corpus,_) ->
        !t.(corpus_index) <- Some (corpus, Unix.gettimeofday ());
        (corpus_index, corpus, corpus_desc)
      | None ->
        match Corpus_desc.load_corpus_opt corpus_desc with
        | None -> raise (Error (`Assoc [("message", `String "[get_corpus] No marshal file"); ("corpus_id", `String (Corpus_desc.get_id corpus_desc))]))
        | Some corpus ->
          !t.(corpus_index) <- Some (corpus, Unix.gettimeofday ());
          (corpus_index, corpus, corpus_desc)

  let refresh_one corpus_id = 
    match String_map.find_opt corpus_id !Global.corpora_map with
    | None -> ()
    | Some (_,corpus_index) -> !t.(corpus_index) <- None

  let refresh_all () =
    for i = 0 to (Array.length !t) - 1 do
      !t.(i) <- None
    done;
    Gc.major ();
end

(* ==================================================================================================== *)
module Client_map = struct
  let (t : Session.t String_map.t ref) = ref (String_map.empty)

  let dump_client_map () =
    printf "------------- <client map> -----------------\n";
    String_map.iter (fun id _ -> printf "[%s]\n" id) !t;
    printf "------------- </client map> ----------------\n"

  (* remove expired items from client_map *)
  let purge () =
    t :=
      String_map.filter
        (fun _ ({Session.last; _}) -> Unix.gettimeofday () -. last < Global.timeout_request)
        !t;
    Table.refresh ();
end
