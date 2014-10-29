Basho bench Riak Set extension
==============================

This is a custom driver to aid in benchmarking and testing Riak in a
setting where we have to use different buckets, and add-only Sets.

For more information about basho bench, please see its
[documentation][docs:basho-bench], and the examples herein in the
`examples/` directory.

 [docs:basho-bench]: http://docs.basho.com/riak/latest/ops/building/benchmarking/

Usage
-----

To build the project, simply type `make`. To run a test, use the
`./basho_bench` binary created, as explained in the documentation.
Generating the summary graphs can be done in the same way as described
there.

Copyright
---------

Copyright © 2014 Gergely Nagy <algernon@madhouse-project.org>, under
the [Apache 2.0][license:apache2] license.

 [license:apache2]: http://www.apache.org/licenses/LICENSE-2.0
