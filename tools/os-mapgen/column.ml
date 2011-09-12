
open Printf
open Stringset

type t = { 
	records : string list ;
	entries : StringSet.t ;
	title : string ;
}

type colinfo = {
	ci_title : string ;
	ci_type : Celltype.t ;
	ci_cardinality : int ;
}

let create csv index =	
	assert(Csv.is_square csv);
	assert(index < Csv.columns csv);
	let nth' line = List.nth line index in
	let records = List.map nth' csv in
	let header = List.hd records in
	let records = List.tl records in
	let entries = StringSet.of_list records 
	in
		{ 
			records = records ;
			entries = entries ;
			title   = header  ;
		}

let dump column =
	List.iter print_endline column.records

let infer_column_type column =
	let types = List.map Celltype.infer column.records in
	let frequencies = Util.frequencies types in
	let frequencies = Util.iteritems frequencies in
	let popular_types = List.sort 
		(fun a b -> compare (snd b) (snd a)) frequencies 
	in
(*	let display_cell_types frequencies =
		List.iter (fun (k, v) -> printf "%s: %d\n" (Celltype.string_of k) v) frequencies
	in *)
	let censored_freqs = 
		List.filter 
			(fun (ty, freq) -> not (Celltype.is_empty ty)) 
			popular_types in

		(* display_cell_types frequencies; *)
		assert (0 < List.length censored_freqs);
		fst (List.hd censored_freqs)

let analyse column =
	let cardinality = StringSet.cardinal column.entries in
		{
			ci_title = column.title;
			ci_type = infer_column_type column;
			ci_cardinality = cardinality;
		}
