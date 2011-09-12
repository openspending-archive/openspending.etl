
type t

type colinfo = {
	ci_title : string ;
	ci_type : Celltype.t ;
	ci_cardinality : int ;
}

val create : Csv.t -> int -> t
val dump : t -> unit
val analyse : t -> colinfo
