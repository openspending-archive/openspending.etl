type colspec
type field

type datatype = String | Float | Currency | Date

(* users should just be able to make one of these themselves *)
type os_field_type =
	| Value of colspec
	| Classifier of string * field list
	| Entity of field list

type os_field_type_type =
	| TValue
	| TClassifier
	| TEntity

type os_field

val datatype : string -> datatype

val colspec : datatype : datatype -> column : string -> 
	default : string -> colspec

val field : colspec : colspec -> constant : string -> 
	name : string -> field

val complex_field : datatype : datatype -> column : string -> default  : string -> constant  : string -> name : string -> field

val os_field : os_name : string -> field_type : os_field_type -> 
	label : string -> description : string -> os_field

val string_of_datatype : datatype -> string

val value_field : os_name:string ->
  label:string ->
  description:string ->
  datatype:datatype -> column:string -> default:string -> os_field

val classifier_field : os_name:string ->
  label:string ->
  description:string -> taxonomy:string -> fields:field list -> os_field

val entity_field :  os_name:string ->
  label:string -> description:string -> fields:field list -> os_field

module Json : sig
	val json_of_os_field : os_field -> Json_type.json_type
	val json_tuple_of_os_field : os_field -> string * Json_type.json_type
    val string_of_fields : os_field list -> string
end
