open Mapping

let test_value_field () =
	let osf = value_field 
		~os_name:"amount"
		~label:"Amount"
		~description:"Amount disbursed"
		~datatype:Float
		~column:"amount"
		~default:""
	in
		Json.json_tuple_of_os_field osf
		

let test_classifier_field () =
	let fields = [ 
		complex_field ~datatype:String ~column:"institution" ~default:"Unknown" ~constant:"" ~name:"label"
	] in
	let osf = classifier_field
		~os_name:"institution"
		~label:"Recipient institution"
		~description:"The institution etc"
		~taxonomy:"nerc-gotw.recipient.institution"
		~fields
	in
		Json.json_tuple_of_os_field osf

let test_entity_field () =
	let fields = [ 
		complex_field ~datatype:String ~column:"spending_source" ~default:"" ~constant:"" ~name:"label";
		complex_field ~datatype:String ~column:"spending_source_id" ~default:"" ~constant:"" ~name:"code";
	] in
	let osf = entity_field
		~os_name:"from"
		~label:"Spender"
		~description:"Name of spending source"
		~fields
	in
		Json.json_tuple_of_os_field osf

let test_all () =
	let o = test_value_field () in
	let o2 = test_classifier_field () in
	let o3 = test_entity_field () in
	let json = Json_type.Object [ o; o2; o3 ] in
		print_endline (Json_io.string_of_json json)
