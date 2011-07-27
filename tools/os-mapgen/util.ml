
let frequencies (l : 'a list) : (('a, int) Hashtbl.t) =
	let tmp : ('a, int) Hashtbl.t = Hashtbl.create 10 in
	let find k = 
		try Hashtbl.find tmp k
		with Not_found -> 0
	in
	let incr k =
		Hashtbl.replace tmp k (1 + (find k))
	in
		List.iter incr l;
		tmp

let iteritems tbl =
	Hashtbl.fold (fun k v acc -> (k, v) :: acc) tbl []

let range min max =
	let rec range min max acc =
		if min > max
		then [] 
		else min :: (range (min + 1) max acc)
	in
		range min max []

let slugify name =
	let regexp1 = Str.regexp " " in
	let regexp2 = Str.regexp "[^A-Za-z0-9_]" in
		String.lowercase (Str.global_replace regexp2 "" 
							  (Str.global_replace regexp1 "_" name))
