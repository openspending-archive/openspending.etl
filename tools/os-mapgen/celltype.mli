
type t =
	| DateTime
	| String
	| CurrencySymbol
	| CurrencyValue
	| Int
	| Float
	| Empty


val infer : string -> t
val string_of : t -> string
val init : unit -> unit
val is_empty : t -> bool
