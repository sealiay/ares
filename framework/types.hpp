
#ifndef _ARES_TYPES_HPP_
#define _ARES_TYPES_HPP_

#include "cstddef"
#include "tuple"
#include "type_traits"
#include "vector"

namespace ares_impl {

	struct error_func_type {
		struct error {};
		typedef error type;
		typedef error map_t;
		typedef error reduce_t;
		typedef error combine_t;
		typedef error arg_t;
		typedef error key_t;
		typedef error val_t;
		typedef error ret_t;
		typedef error pair_t;
		typedef error setup_t;
	};

	template <typename T> struct remove_cref {
		typedef typename std::remove_cv<typename std::remove_reference<T>::type>::type type;
	};

	template <typename> struct function_type;
	template <typename O, typename  R, typename ... Ps>
	struct function_type<R (O::*)(Ps...)> {
		typedef R ret_t;
		template <size_t I> using arg_t =
				typename std::tuple_element<I, std::tuple<Ps...>>::type;
		static constexpr size_t n_args = sizeof...(Ps);
	};

	template <typename F>
	struct function_type_without_cref {
		typedef F func_t;

		typedef typename remove_cref<typename func_t::ret_t>::type ret_t;

		template <size_t I> using arg_t =
				typename remove_cref<typename func_t::template arg_t<I>>::type;
		static constexpr size_t n_args = func_t::n_args;
	};


#define def_has(elem)								\
template <typename T> static std::true_type 		\
has_##elem##_helper(decltype(&T::elem));			\
template <typename T> static std::false_type 		\
has_##elem##_helper(...);							\
template <typename T> using has_##elem = 			\
decltype(has_##elem##_helper<T>(nullptr))

	def_has(write_to);
	def_has(map);
	def_has(reduce);
	def_has(combine);
	def_has(setup);

#undef def_has

#define def_func_type(what)								\
	template <typename T> using what##_func_type =		\
	typename std::conditional<has_##what<T>::value,		\
	what##_func_type_impl<T>, error_func_type>::type


	template <typename T> struct setup_func_type_impl {
		typedef function_type<decltype(&T::setup)> func_t;
		typedef function_type_without_cref<func_t> ftncr_t;
		typedef typename ftncr_t::template arg_t<0> type;
	};

	def_func_type(setup);

	template <typename M> struct map_func_type_impl {
		typedef M map_t;
		typedef function_type<decltype(&map_t::map)> func_t;
		typedef function_type_without_cref<func_t> ftncr_t;

		static_assert(func_t::n_args == 2, "map should have 2 parameters");

		typedef typename ftncr_t::template arg_t<0> arg_t;
		typedef typename ftncr_t::template arg_t<1>::value_type::first_type key_t;
		typedef typename ftncr_t::template arg_t<1>::value_type::second_type val_t;

		typedef typename setup_func_type<map_t>::type setup_t;
	};

	def_func_type(map);

	template <typename R> struct reduce_func_type_impl {
		typedef R reduce_t;
		typedef function_type<decltype(&reduce_t::reduce)> func_t;
		typedef function_type_without_cref<func_t> ftncr_t;

		static_assert(func_t::n_args == 2, "reduce should have 2 parameters");

		typedef typename ftncr_t::ret_t ret_t;
		typedef typename ftncr_t::template arg_t<0> key_t;
		typedef typename ftncr_t::template arg_t<1>::value_type val_t;

		typedef typename setup_func_type<reduce_t>::type setup_t;
	};

	def_func_type(reduce);

	template <typename C> struct combine_func_type_impl {
		typedef C combine_t;
		typedef function_type<decltype(&combine_t::reduce)> func_t;
		typedef function_type_without_cref<func_t> ftncr_t;

		static_assert(func_t::n_args == 2, "combine should have 2 parameters");

		typedef typename ftncr_t::ret_t pair_t;
		typedef typename ftncr_t::template arg_t<0> key_t;
		typedef typename ftncr_t::template arg_t<1>::value_type val_t;

		typedef typename setup_func_type<combine_t>::type setup_t;
	};

	def_func_type(combine);

#undef def_func_type

	using std::pair;
	using std::make_pair;

	template <typename T> using collection = std::vector<T>;
	template <typename K, typename V> using collection2 = collection<pair<K, V>>;

	template <typename M, typename R = M, typename C = R> struct job {
		typedef map_func_type<M> map_func;
		typedef reduce_func_type<R> reduce_func;
		typedef combine_func_type<C> combine_func;

		typedef typename map_func::map_t map_t;
		typedef typename reduce_func::reduce_t reduce_t;
		typedef typename combine_func::combine_t combine_t;

		typedef typename map_func::arg_t arg_t;
		typedef typename map_func::key_t key_t;
		typedef typename map_func::val_t val_t;
		typedef typename reduce_func::ret_t ret_t;
		typedef pair<key_t, val_t> pair_t;

		typedef typename map_func::setup_t m_setup_t;
		typedef typename reduce_func::setup_t r_setup_t;
		typedef typename combine_func::setup_t c_setup_t;

		typedef collection<arg_t> arg_cc_t;
		typedef collection<key_t> key_cc_t;
		typedef collection<val_t> val_cc_t;
		typedef collection<ret_t> ret_cc_t;
		typedef collection<pair_t> pair_cc_t;

		static_assert(has_map<M>::value,
				"map type must have function void map(I, collection2<K, V> &)");

		static_assert(has_reduce<R>::value,
				"reduce type must have function O reduce(K, collection<V>)");

		static_assert(std::is_same<key_t, typename reduce_func::key_t>::value,
				"map/reduce key type should match");

		static_assert(std::is_same<val_t, typename reduce_func::val_t>::value,
				"map/reduce value type should match");

		static_assert(!has_combine<C>::value ||
				std::is_same<pair_t, typename combine_func::pair_t>::value,
				"combine input/output key/value type should match");

		static_assert(!has_combine<C>::value ||
				std::is_same<key_t, typename combine_func::key_t>::value,
				"map/combine key type should match");

		static_assert(!has_combine<C>::value ||
				std::is_same<val_t, typename combine_func::val_t>::value,
				"map/combine value type should match");
	};
}


#endif // _ARES_TYPES_HPP_
