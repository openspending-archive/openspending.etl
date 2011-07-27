open Json_type (* imports something conflicting with "String" below *)

type datatype = String | Float | Currency | Date

type colspec = {
	cs_datatype : datatype ;
	cs_column : string ;
	cs_default : string ;
}

type field = {
	f_constant : string ;
	f_name : string ;
	f_colspec : colspec ;
}

type os_field_type =
	| Value of colspec
	| Classifier of string * field list
	| Entity of field list

type os_field_type_type =
	| TValue
	| TClassifier
	| TEntity

type os_field = 
	{
		osf_os_name : string ;
		osf_type : os_field_type ;
		osf_label : string ;
		osf_description : string ;
	}

let datatype = function
	| "string" -> String
	| "float" -> Float
	| "currency" -> Currency
	| "date" -> Date
	| _ -> raise Not_found

let colspec ~datatype ~column ~default =
	{
		cs_datatype = datatype;
		cs_column = column;
		cs_default = default;
	}

let field ~colspec ~constant ~name =
	{
		f_colspec = colspec;
		f_name = name;
		f_constant = constant;
	}

let complex_field ~datatype ~column ~default ~constant ~name =
	{
		f_colspec = {
			cs_datatype = datatype;
			cs_column = column;
			cs_default = default;
		};
		f_name = name;
		f_constant = constant;
	}

let os_field ~os_name ~field_type ~label ~description =
	{
		osf_os_name = os_name;
		osf_type = field_type;
		osf_label = label;
		osf_description = description;
	}

let string_of_datatype = function
	| String -> "string"
	| Date -> "date"
	| Currency -> "currency"
	| Float -> "float"

let string_of_field_type = function
	| Value _ -> "value"
	| Classifier _ -> "classifier"
	| Entity _ -> "entity"


let value_field ~os_name ~label ~description ~datatype ~column ~default =
	let cs = colspec ~datatype ~column ~default in 
		os_field ~os_name ~field_type:(Value cs) ~label ~description

let classifier_field ~os_name ~label ~description ~taxonomy ~fields =
	os_field ~os_name ~field_type:(Classifier (taxonomy, fields)) ~label ~description

let entity_field ~os_name ~label ~description ~fields =
	os_field ~os_name ~field_type:(Entity fields) ~label ~description


module Json : sig
	val json_of_os_field : os_field -> Json_type.json_type
	val json_tuple_of_os_field : os_field -> string * Json_type.json_type
    val string_of_fields : os_field list -> string
end = struct
	let json_of_colspec cs =
		[ ("datatype", Json_type.String (string_of_datatype cs.cs_datatype) );
		  ("default_value", Json_type.String cs.cs_default );
		  ("column", Json_type.String cs.cs_column ); ]

	let json_of_osfield_keys osf =
		[ ("type", Json_type.String (string_of_field_type osf.osf_type) );
		  ("description", Json_type.String osf.osf_description );
		  ("label", Json_type.String osf.osf_label ); ]

	let json_of_value_field os_field colspec =
		let csj = json_of_colspec colspec in
		let osfj = json_of_osfield_keys os_field in
			Object (osfj @ csj)

	let json_of_classifier_field os_field ?taxonomy fields =
		let osfj = json_of_osfield_keys os_field in
		let fieldj = 
			List.map (fun f ->
					  Object (
						  ("constant", Json_type.String f.f_constant) ::
					      ("name", Json_type.String f.f_name) ::
						  json_of_colspec f.f_colspec
					  )
					 ) fields in
		let fieldsj = ("fields", Array fieldj) in
		let tail = 
			match taxonomy with
				| None -> osfj
				| Some tx -> ("taxonomy", Json_type.String tx) :: osfj 
		in	
			Object (fieldsj :: tail)

	let json_of_os_field osf =
		match osf.osf_type with
			| Value v -> json_of_value_field osf v
			| Classifier (tx, c) -> json_of_classifier_field osf ~taxonomy:tx c
			| Entity fields -> json_of_classifier_field osf fields

	let json_tuple_of_os_field osf =
		( osf.osf_os_name, json_of_os_field osf )

	let string_of_fields osfs =
		Json_io.string_of_json (Object (List.map json_tuple_of_os_field osfs))

end
