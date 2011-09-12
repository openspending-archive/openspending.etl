open Column

(*

1) attempt to strip out the currency field [todo]
2) attempt to find date fields - we have to bail if we can't do this
3) make everything else a classifier or value
     (cardinality > row_count / 2) ? value : classifier
4) add two dummy entities with no underlying columns

*)

let (/..) a b = (float_of_int a) /. (float_of_int b)

type col =
		{ c_name : string; (* name as in orig CSV *)
		  c_datatype : Mapping.datatype;
		  c_type : Mapping.os_field_type_type;
		}

type mapping =
		{ m_from : col;
		  m_to : col;
		  m_date : col;
		  m_currency : col option;
		  m_others : col list;
		}

let no_column  = { c_name = "NONE"; c_datatype = Mapping.String;
				   c_type = Mapping.TValue }
let dummy_from = { c_name = "from"; c_datatype = Mapping.String;
				   c_type = Mapping.TEntity }
let dummy_to   = { c_name = "to"; c_datatype = Mapping.String;
				   c_type = Mapping.TEntity }

let default_mapping = { 
	m_from = dummy_from;
	m_to = dummy_to;
	m_date = no_column;
	m_currency = None;
	m_others = [];
}

let add_currency ~column_info ~acc =
	let l1, l2 = List.partition (
		fun elt -> elt.ci_type = Celltype.CurrencySymbol
	) column_info
	in
		match l1 with
			| [] -> acc, l2
			| [c] -> let curr_col = { 
				  c_name = c.ci_title;
				  c_datatype = Mapping.Currency;
				  c_type = Mapping.TValue;
			  } in
				  { acc with m_currency = Some curr_col }, l2
			| _ -> assert false

let add_date ~column_info ~acc =
	let l1, l2 = List.partition (
		fun elt -> elt.ci_type = Celltype.DateTime
	) column_info
	in
		assert (1 = List.length l1);
		let col = List.hd l1 in
		let date_col = {
			c_name = col.ci_title;
			c_datatype = Mapping.Date;
			c_type = Mapping.TValue;
		} in
		{ acc with m_date = date_col }, l2

let tot = function
	| Celltype.String -> Mapping.String
	| Celltype.Empty -> assert false
	| Celltype.DateTime -> Mapping.Date
	| Celltype.CurrencyValue -> Mapping.Currency
	| Celltype.CurrencySymbol -> Mapping.Float
	| Celltype.Int -> Mapping.Float
	| Celltype.Float -> Mapping.Float

let calculate_other_columns ~row_count ~column_info =
	List.map ( 
		fun ci ->
			{ 
				c_name = ci.ci_title;
				c_datatype = tot ci.ci_type;
				c_type = match ((ci.ci_cardinality /.. row_count) > 0.5) with
					| true -> Mapping.TValue
					| false -> Mapping.TClassifier
			}		
	) column_info
		

let add_others ~row_count ~column_info ~acc =
	let others = calculate_other_columns ~row_count ~column_info in
		{ acc with m_others = others }, []

let make_mapping ~row_count ~column_info =
	let acc = default_mapping in
	let acc, column_info = add_date ~column_info ~acc in
	let acc, column_info = add_currency ~column_info ~acc in
	let acc, column_info = add_others ~row_count ~column_info ~acc in
		acc

let rename c =
	(Util.slugify c.c_name, c)

let process ~row_count ~column_info =
	let mapping = make_mapping ~row_count ~column_info in 
	let tmp = [ rename mapping.m_from;
				rename mapping.m_to;
				rename mapping.m_date; ] in
	let tmp = match mapping.m_currency with
		| None -> tmp
		| Some c -> (rename c) :: tmp in
		tmp @ (List.map rename mapping.m_others)
			
