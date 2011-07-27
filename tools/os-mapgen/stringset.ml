
module StringSetBase = Set.Make (
	struct
		type t = string
		let compare = compare
	end 
)

module StringSet =
	struct
		include StringSetBase

		let of_list elts =
			List.fold_left 
				(fun acc s -> StringSetBase.add s acc) 
				StringSetBase.empty elts
	end
