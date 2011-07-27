open Printf
open Json_type
open Json_io
open Column
open Heuristics

let usage () =
	printf "Usage: %s filename" Sys.argv.(0)

type csv_info = {
	csv_lines : int;
	csv_columns : int;
	csv_colinfo : Column.colinfo list
}

let process_csv csv =
	let lines = Csv.lines csv in
	let columns = Csv.columns csv in 
		(* printf "CSV lines: %d\nCSV columns: %d\n\n" lines columns; *)
		assert(lines >= 2);
		assert(columns > 0);
		assert(Csv.is_square csv);

		let analyses =
			List.map (fun i ->
						  let column = Column.create csv i in
							  Column.analyse column
					 ) (Util.range 0 (columns - 1))
		in
			{ csv_lines = lines;
			  csv_columns = columns;
			  csv_colinfo = analyses;
			}

let read_stdin () =
	let csv = Csv.load_in stdin in
		process_csv csv

let display_text ci =
	let coltype = Celltype.string_of ci.ci_type in
		Printf.printf "%s: %s\n" ci.ci_title coltype

let osf_of_col_tuple (os_name, col) = 
	let label = "Unlabeled" in
	let description = "Undescribed" in
	let default = "" in
	let column = col.c_name in
	let constant = "" in
	let taxonomy = "unknown" in
	let datatype = col.c_datatype in
	let fields = [
		Mapping.complex_field ~datatype ~column ~default
			~constant ~name:"label"
	] in
		match col.c_type with
			| Mapping.TValue -> 
				  Mapping.value_field ~os_name ~label ~description
					  ~datatype ~column ~default
			| Mapping.TClassifier -> 
				  Mapping.classifier_field ~os_name ~label ~description
					  ~taxonomy ~fields
			| Mapping.TEntity -> 
				  Mapping.entity_field ~os_name ~label ~description ~fields

let display_json csv =
	let results = Heuristics.process ~row_count:csv.csv_lines 
			   ~column_info:csv.csv_colinfo in
	let fields = List.map osf_of_col_tuple results in
		print_endline (Mapping.Json.string_of_fields fields)

let read_stdin ~json () =
	let csv = read_stdin () in
		match json with
			| true -> display_json csv
			| false -> List.iter display_text csv.csv_colinfo

let main () =
	match Sys.argv with
		| [| _ |] -> read_stdin ~json:true ()
		| [| _ ; "--test-mapping" |] -> Test_mapping.test_all ()
		| [| _ ; "--text" |] -> read_stdin ~json:false ()
		| _ -> usage ()

let _ = main ()
