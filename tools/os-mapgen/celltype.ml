
open Stringset

let currencies = ref StringSet.empty

let init () =
	let currencies' = [ "GBP"; "USD"; "EUR"; "AUD"; ] in
		currencies := StringSet.of_list currencies'

type t =
	| DateTime
	| String
	| CurrencySymbol
	| CurrencyValue
	| Int
	| Float
	| Empty

let string_of = function
	| String -> "string"
	| Empty -> "empty"
	| DateTime -> "temporal"
	| CurrencyValue -> "amount"
	| CurrencySymbol -> "currency"
	| Int -> "int"
	| Float -> "float"

let patterns = [
	(Empty, "");
	(DateTime, "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");
	(DateTime, "[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]");
	(CurrencyValue, "-?[0-9]+\\(\\.[0-9][0-9]\\)?");
	(CurrencyValue, "-?[0-9]*\\(\\.[0-9][0-9]\\)");
	(Int, "-?[0-9]+");
	(Float, "-?[0-9]*\\.[0-9]+");
	(String, ".*");
]

let patterns =
	let anchor s = "^" ^ s ^ "$" in
		List.map (fun (ty, regex) -> (ty, Str.regexp (anchor regex))) patterns 

let is_currency_symbol s =
	StringSet.mem s !currencies

let match_regexps s =
	let rec match_regexps = function
		| [] -> raise Not_found
		| (ty, reg) :: tl ->
			  if (Str.string_match reg s 0)
			  then ty
			  else match_regexps tl
	in
		match_regexps patterns

let infer s =
	if is_currency_symbol s
	then CurrencySymbol
	else match_regexps s

let is_empty = function
	| Empty -> true
	| _ -> false	

let _ = init () (* obviously this should be run from somewhere else *)

