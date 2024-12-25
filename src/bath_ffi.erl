-module(bath_ffi).

-export([unsafe_coerce/1]).

unsafe_coerce(Term) ->
    Term.
