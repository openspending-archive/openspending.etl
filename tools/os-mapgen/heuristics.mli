
type col =
		{ c_name : string; (* name as in orig CSV *)
		  c_datatype : Mapping.datatype;
		  c_type : Mapping.os_field_type_type;
		}

val process :
	row_count : int ->
	column_info : Column.colinfo list -> 
	(string * col) list
