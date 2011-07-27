
Prerequisites:

For build:
* OCaml compiler
* OCamlMakefile
* OCaml standard library
* the OCaml CSV library (https://forge.ocamlcore.org/frs/?group_id=113)

The compiled binary needs none of these prereqs, not even the libraries, as
it is staticly linked.

To build:

$ ln -sf /usr/lib/ocamlmakefile/OCamlMakefile OCamlMakefile
$ make clean
$ make bc               (or make nc, for a faster binary)

To run:

$ ./os-mapgen < openspending-dataset.csv > mapping.json

