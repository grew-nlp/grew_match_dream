open Printf
open Dep2pictlib
open Dream_utils
open Grewlib

module String_set = struct
  include String_set
  let to_json t = `List (List.map (fun x -> `String x) (elements t))
end

module String_map = struct
  include String_map
  let to_json value_to_json t =
    let assoc_list = fold
      (fun key value acc ->
        (key, value_to_json value) :: acc
      ) t [] in 
     `Assoc assoc_list
end

let parse_json_string_list param_name string =
  let open Yojson.Basic.Util in
  try string |> Yojson.Basic.from_string |> to_list |> List.map to_string
  with
  | Type_error _ -> error "Ill-formed parameter `%s`. It must be a list of strings" param_name
  | Yojson.Json_error _ -> error "`%s` parameter is not a valid JSON data" param_name

let json_assoc_add (key : string) (value: Yojson.Basic.t) (json: Yojson.Basic.t) =
  match json with
  | `Assoc list -> `Assoc ((key, value) :: list)
  | _ -> error "[json_assoc_add] data is not an assoc list"

(* ================================================================================ *)
(* Utils *)
(* ================================================================================ *)
let folder_iter fct folder =
  let dh = Unix.opendir folder in
  try
    while true do
      match Unix.readdir dh with
      | "." | ".." -> ()
      | x -> fct x
    done;
    assert false
  with
  | End_of_file -> Unix.closedir dh

let folder_fold fct init folder =
  let dh = Unix.opendir folder in
  let acc = ref init in
  try
    while true do
      match Unix.readdir dh with
      | "." | ".." -> ()
      | file -> acc := fct file !acc
    done;
    assert false
  with
  | End_of_file -> Unix.closedir dh; !acc

(* ================================================================================ *)
(* Array_ *)
(* ==================================================================================================== *)
module Array_ = struct
  let find a test =
    let rec loop n =
      if test a.(n) then n
      else loop (n+1) in
    loop 0 
end

(* ================================================================================ *)
(* Redundant *)
(* ================================================================================ *)
module Redundant = struct
  let bound = 20
  let storage = ref Int_map.empty

  let record request =
    let hash = CCHash.string request in
    let new_count = match Int_map.find_opt hash !storage with None -> 1 | Some i -> (i+1) mod bound in
    storage := Int_map.add hash new_count !storage;
    begin
      if new_count = 0
      then Log.info "<redundant> request=%s" request
    end;
    new_count
end



(* ================================================================================ *)
(* Misc functions *)
(* ================================================================================ *)
let get_attr_opt field json =
  let open Yojson.Basic.Util in
    match json |> member field with
    | `Null -> None
    | j -> Some j

let get_attr field json =
  match get_attr_opt field json with
  | Some s -> s
  | None -> raise (Error (`Assoc [("message", `String (sprintf "No field `%s`" field)); ("json", json)]))

let get_string_attr_opt field json =
  let open Yojson.Basic.Util in
  try json |> member field |> to_string_option
  with Type_error (msg,_) -> 
    raise (
      Error (
        `Assoc 
          [
            ("error", `String (sprintf "Cannot get string field `%s`" field));
            ("message", `String msg);
            ("json", json)
          ]
      )
    )

let get_string_attr field json =
  match get_string_attr_opt field json with
  | Some s -> s
  | None -> raise (Error (`Assoc [("message", `String (sprintf "No field `%s`" field)); ("json", json)]))

let get_int_attr field json =
  let open Yojson.Basic.Util in
  try json |> member field |> to_int
  with Type_error _ ->  raise (Error (`Assoc [("message", `String (sprintf "No int field `%s`" field)); ("json", json)]))

let get_path_attr field json =
  let open Yojson.Basic.Util in
  try json |> member field |> to_list |> List.map to_int
  with Type_error _ ->  raise (Error (`Assoc [("message", `String (sprintf "No path (i.e. int list) field `%s`" field)); ("json", json)]))

let get_named_path_attr field json =
  let open Yojson.Basic.Util in
  try 
    json 
    |> member field 
    |> to_list 
    |> List.map 
      (function 
      | `String "__undefined__" -> None 
      | `String s -> Some s 
      | _ -> raise (Error (`Assoc [("message", `String (sprintf "Some element is not of type string in field `%s`" field)); ("json", json)]))
      ) 
  with Type_error _ ->  raise (Error (`Assoc [("message", `String (sprintf "No path (i.e. string list) field `%s`" field)); ("json", json)]))

let get_bool_attr_opt field json =
  let open Yojson.Basic.Util in
  try json |> member field |> to_bool_option
  with Type_error (msg,_) -> 
    raise (
      Error (
        `Assoc 
          [
            ("error", `String (sprintf "Cannot get bool field `%s`" field));
            ("message", `String msg);
            ("json", json)
          ]
      )
    )

let get_clust_item_list param = 
  (match (get_string_attr_opt "clust1_key" param, get_string_attr_opt "clust1_whether" param) with
  | (None, None) -> []
  | (Some k, _) -> [k]
  | (_, Some w) -> [sprintf "{%s}" w])
  @
  (match (get_string_attr_opt "clust2_key" param, get_string_attr_opt "clust2_whether" param) with
  | (None, None) -> []
  | (Some k, _) -> [k]
  | (_, Some w) -> [sprintf "{%s}" w])

let json_string_of_key = function Some v -> `String v | None -> `String "__undefined__"
let json_values_sizes values_sizes =
  `List
    (Array.to_list
      (Array.map
        (fun (clust_value,size) ->
          `Assoc [
            ("value", json_string_of_key clust_value);
            ("size", `Int size)
          ]
        ) values_sizes
      )
    )

let concat_filenames = function
  | [] -> failwith "empty list in concat_filenames"
  | h::t -> List.fold_left Filename.concat h t 

let datadir uuid = concat_filenames [Dream_config.get_string "storage"; "data"; uuid]

(* Use PHP way to compute uniqid (see: https://www.php.net/manual/en/function.uniqid.php#95001)*)
let php_uniqid () = 
  let ts = Unix.gettimeofday () in 
  let seconds = floor ts in
  sprintf "%8x%05x" (int_of_float seconds) (int_of_float ((ts-.seconds) *. 1000000.))


(* Build a map [corpus_id ==> corpus_desc] with all corpora described in [corpusbank] folder *)
let load_corpusbank corpusbank : (Corpus_desc.t * int) String_map.t = 
  let map = ref String_map.empty in
  let counter = ref 0 in

  try 
    let file_seq = CCIO.File.read_dir corpusbank in
    let rec loop () =
      match file_seq () with
      | None -> () 
      | Some file ->
        begin
          if Filename.check_suffix file ".json" 
          then
            let desc_list = Corpus_desc.load_json ~env:(Dream_config.get_env ()) (Filename.concat corpusbank file) in 
            List.iter 
              (fun desc -> 
                map := String_map.add (Corpus_desc.get_id desc) (desc, !counter) !map;
                incr counter
              ) desc_list
          end;
          loop () in
    loop ();
    !map
  with Sys_error _ -> !map

module Draw_config = struct
  type t = { lemma: bool; upos:bool; xpos:bool; features:bool; tf_wf:bool; context:bool; pid: bool;}

  let from_param param = {
    lemma = (match get_bool_attr_opt "lemma" param with Some v -> v | None -> false);
    upos = (match get_bool_attr_opt "upos" param with Some v -> v | None -> false);
    xpos = (match get_bool_attr_opt "xpos" param with Some v -> v | None -> false);
    features = (match get_bool_attr_opt "features" param with Some v -> v | None -> false);
    tf_wf = (match get_bool_attr_opt "tf_wf" param with Some v -> v | None -> false);
    context = (match get_bool_attr_opt "context" param with Some v -> v | None -> false);
    pid = (match get_bool_attr_opt "pid" param with Some v -> v | None -> true);

  }
  let filter t = function
    | "phon" | "form" -> true
    | "lemma" -> t.lemma
    | "upos" | "cat" -> t.upos
    | "xpos" | "pos" -> t.xpos
    | "textform" | "wordform" -> t.tf_wf
    | "SpaceAfter" -> false
    | "AlignBegin" | "AlignEnd" -> false
    | "_speaker" | "_start" | "_stop" -> false 
    | _ -> t.features
end




(* ============================================================================================================================ *)
let save_dep uuid ?audio_info rtl base sent_id sentence meta dep =
  try 
    let folder = datadir uuid in
    let basename = sprintf "%s.svg" base in
    let _ = Dep2pictlib.save_svg ~filename:(Filename.concat folder basename) (Dep2pictlib.from_dep ~rtl dep) in
    let shift = Dep2pictlib.highlight_shift () in
    let data = `Assoc (CCList.filter_map CCFun.id [
        Some ("kind", `String "ITEM");
        Some ("filename", `String (Filename.concat uuid basename));
        Some ("sent_id", `String sent_id);
        Some ("sentence", `String sentence);
        Some ("meta", `Assoc (List.map (fun (k,v) -> (k, `String v)) meta));
        CCOption.map (fun v -> ("shift", `Float v)) shift;
        CCOption.map (fun s -> ("audio", `String s)) audio_info;
      ]) in 
    data
  with Dep2pictlib.Error json -> raise (Error (`Assoc [("message", `String "Dep2pict error"); ("sent_id", `String sent_id); ("json", json)]))

(* ============================================================================================================================ *)
let save_dot uuid base sent_id graph sentence meta dot =
  let folder = datadir uuid in
  let temp_file_name,out_ch = Filename.open_temp_file ~mode:[Open_rdonly;Open_wronly;Open_text] "grew_" ".dot" in
  fprintf out_ch "%s" dot;
  close_out out_ch;
  let basename = sprintf "%s.svg" base in
  ignore (Sys.command(sprintf "dot -Tsvg -o %s %s " (Filename.concat folder basename) temp_file_name));
  let data = `Assoc [
      ("kind", `String "ITEM");
      ("filename", `String (Filename.concat uuid basename));
      ("sent_id", `String sent_id);
      ("sentence", `String sentence);
      ("meta", `Assoc (List.map (fun (k,v) -> (k, `String v)) meta));
      ("code", match Graph.get_meta_opt "code" graph with Some s -> `String s | None -> `Null);
      ("url", match Graph.get_meta_opt "url" graph with Some s -> `String s | None -> `Null);
    ] in
  data


(* Returns a float value corresponding to the left part of an interval like ]-∞, 3] or [1,2] 
   raises Failure if the input string in not an interval *)
exception Not_interval
let get_left_interval s =
  if s = "__*__" (* be sure that merge columns stay at the end *)
  then Float.infinity
  else 
  try 
    let _ = Str.search_forward (Str.regexp {|[][]\([^,]*\),|}) s 0 in
        match Str.matched_group 1 s with
        | "-∞" -> Float.neg_infinity
        | s -> float_of_string s 
  with Not_found -> raise Not_interval

(* sorting of clustering keys: as intervals, as floats, or alphabetically *)
let cluster_sort arr =
  (* step 1) try to sort all the array as intervals, raise Not_interval if some elt is not an interval *)
  try Array.sort 
    (fun x y -> match (x,y) with
      | ((Some k1,_), (Some k2,_)) -> Stdlib.compare (get_left_interval k1) (get_left_interval k2)
      | _ -> raise Not_interval (* None is not a valid interval *)
    ) arr
  with Not_interval -> 

    (* step 2) try to sort all the array as floats, raise Failure if some elt is not a float *)
    try Array.sort 
      (fun x y -> match (x,y) with
        | ((Some k1,_), (Some k2,_)) -> Stdlib.compare (float_of_string k1) (float_of_string k2)
        | _ -> raise (Failure "not_all_float")
      ) arr
      with Failure _ -> (* either "not_all_float" or "float_of_string" *)

      (* step 3) sort alphabetically *)
      Array.sort (fun (k1,_) (k2,_) -> Stdlib.compare k1 k2) arr 


(* Given two arrays containing the same elements without repetition, 
  [compute_permutation] returns a int array describing the permutation from [array1] to [array2]
  Example:
  `compute_permutation [|"A"; "B"; "C"; "D"|] [|"D"; "A"; "C"; "B"|]` returns [|3; 0; 2; 1|]
*)
let compute_permutation array1 array2 =
  let len = Array.length array1 in
  let index_map = Hashtbl.create len in
    for i = 0 to len - 1 do
      Hashtbl.add index_map array1.(i) i
    done;
    let permutation = Array.make len 0 in
    for i = 0 to len - 1 do
      permutation.(i) <- Hashtbl.find index_map array2.(i)
    done;
    permutation

let key_sort keys =
  (* step 1) try to sort all the array as intervals, raise Not_interval if some elt is not an interval or __*__ *)
  try Array.sort 
    (fun x y -> match (x,y) with
      | (Some k1, Some k2) -> Stdlib.compare (get_left_interval k1) (get_left_interval k2)
      | _ -> raise Not_interval
    ) keys
  with Not_interval -> 
    (* step 2) try to sort all the array as floats, raise Failure if some elt is not a float *)
    try Array.sort 
      (fun x y -> match (x,y) with
        (* __*__ s the highest possible value, it is always the last row/col *)
        | (Some "__*__", Some _) -> 1
        | (Some _, Some "__*__") -> -1
        | (Some k1, Some k2) -> Stdlib.compare (float_of_string k1) (float_of_string k2)
        | _ -> raise (Failure "not all floats")
      ) keys
    with Failure _ ->

      (* 3) sort alphabetically *)
      Array.sort (fun k1 k2 -> match (k1, k2) with
        (* __*__ s the highest possible value, it is always the last row/col *)
        | (Some "__*__", _) -> 1 
        | (_, Some "__*__") -> -1
        | _ -> Stdlib.compare k1 k2) keys

(* from an array of (key * 'a), returns the permutation (int array)
   describing the sorting of the keys (taking into account intervals, number or string keys)
   return a JSON list of int, ready to be sent to grew-match frontend *)
let permutation_json_list arr =
  let keys = Array.map fst arr in
  let _ = key_sort keys in
  compute_permutation (Array.map fst arr) keys
  |> Array.map (fun i -> `Int i)
  |> Array.to_list

