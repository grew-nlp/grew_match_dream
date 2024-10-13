open Grewlib

module Global = struct
  let refresh_frequency = 300 (* delay between two purges of the table *)
  let timeout_request = 300.  (* nb of seconds before a requet is removed from Client_map *)
  let timeout_search = 10.    (* nb of seconds before Search operation is stopped  *)
  let nbre_sol_page = 10      (* nb of images generated at each "NEXT" command *)
  let max_results = 1000      (* Max number of occurrences computed *)
  let max_grid_size = 12      (* if this number of rows / columns is more than this number, folding is applied *)
  let folded_size = 8         (* when folding grid, keep this number of columns, fold in the next one *)
  
  let (corpora_map : (Corpus_desc.t * int) String_map.t ref) = ref String_map.empty
end (* module Global *)
