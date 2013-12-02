
#ifndef _ARES_WORKFLOW_HPP_
#define _ARES_WORKFLOW_HPP_

#include "mpi.hpp"

#include <typeindex>
#include <unordered_map>

namespace ares_impl {

	class work_flow {
	public:
		mpi_controller mpi;

		byte_array mapped_data;
		byte_array m_side_data;
		byte_array r_side_data;
		byte_array c_side_data;

		static work_flow *&instance() {
			static work_flow *impl;
			return impl;
		}

		typedef void *(work_flow::*handler_t)(void *);
		std::unordered_map<std::type_index, size_t> index;
		std::vector<handler_t> hlist;

		template <typename M> void m_register(std::true_type) {
			index[typeid(map_func_type<M>)] = hlist.size();
			hlist.push_back(&work_flow::do_map<M>);
		}
		template <typename> void m_register(std::false_type) {}

		template <typename R> void r_register(std::true_type) {
			index[typeid(reduce_func_type<R>)] = hlist.size();
			hlist.push_back(&work_flow::do_reduce<R>);
		}
		template <typename> void r_register(std::false_type) {}

		template <typename C> void c_register(std::true_type) {
			index[typeid(combine_func_type<C>)] = hlist.size();
			hlist.push_back(&work_flow::do_combine<C>);
		}
		template <typename> void c_register(std::false_type) {}

		template <typename T, typename ... Ts> void register_type(T *, Ts *...ts) {
			m_register<T>(has_map<T>());
			r_register<T>(has_reduce<T>());
			c_register<T>(has_combine<T>());
			register_type(ts...);
		}
		void register_type() {}

		template <typename T>
		void scatter(const collection<T> &arg_cc) {
			typedef T arg_t;
			typedef collection<arg_t> arg_cc_t;

			command head;
			head.code = opt_code::map_data;
			mpi.bcast(head);

			size_t size = mpi.size();
			byte_array datas[size];

			size_t curr = 0;

			for (size_t k = 0; k < size; ++k) {
				size_t one = (arg_cc.size() + size - 1) / size;
				one = std::min(one, arg_cc.size() - curr);
				datas[k].write(one);
				for (size_t i = curr; i < curr + one; ++i) {
					datas[k].write(arg_cc[i]);
				}
				curr += one;
			}

			mpi.scatter(datas, mapped_data);
		}

		template <typename T> void set_side_data(const T &data, byte_array &bytes, opt_code opt) {
			bytes.clear();
			bytes.write(data);

			command head;
			head.code = opt;
			head.value = bytes.size();
			mpi.bcast(head);

			mpi.bcast(bytes, bytes.size());
		}

		handler_t get_handler(size_t idx, int shift) {
			return hlist[(idx >> shift) & 0xffffUL];
		}

		void do_job(size_t idx, byte_array final[]) {
			void *p = nullptr;
			handler_t m = get_handler(idx, 32);
			handler_t r = get_handler(idx, 16);
			handler_t c = get_handler(idx, 00);

			p = (this->*m)(p);
			if ( c != nullptr ) {
				p = (this->*c)(p);
			}
			p = (this->*r)(p);

			byte_array * result= (byte_array *)p;
			mpi.gather(*result, final);
			delete result;
		}

		template <typename M, typename R, typename C>
		typename job<M, R, C>::ret_cc_t do_run() {
			typedef job<M, R, C> job;
			typedef typename job::ret_cc_t ret_cc_t;

			size_t m_idx, r_idx, c_idx;
			m_idx = index[typeid(typename job::map_func)];
			r_idx = index[typeid(typename job::reduce_func)];
			c_idx = index[typeid(typename job::combine_func)];

			if ( m_idx == 0 || r_idx == 0 ) {
				fprintf(stderr, "unregistered map or reduce type\n");
				return ret_cc_t();
			}

			command head;
			head.code = opt_code::start;
			head.value = (m_idx << 32) | (r_idx << 16) | c_idx;
			mpi.bcast(head);

			size_t size = mpi.size();
			byte_array datas[size];
			do_job(head.value, datas);

			ret_cc_t ret_cc;
			for (size_t k = 0; k < size; ++k) {
				byte_array &x = datas[k];
				ret_cc_t cc = x.read<ret_cc_t>();
				std::move(cc.begin(), cc.end(), std::back_inserter(ret_cc));
			}
			return std::move(ret_cc);
		}

		void exit_all(int code) {
			command head;
			head.code = opt_code::exit;
			head.value = code;
			mpi.bcast(head);
		}

		void finalize() {
			delete this;
			MPI_Finalize();
		}

		static void when_exit() {
			instance()->exit_all(0);
			instance()->finalize();
		}

		void listen() {
			command head;
			mpi.bcast(head);

			switch (head.code) {
			case opt_code::exit:
				finalize();
				exit((int)head.value);
			case opt_code::start:
				do_job(head.value, nullptr);
				break;
			case opt_code::map_data:
				mapped_data.clear();
				mpi.scatter(nullptr, mapped_data);
				break;
			case opt_code::m_side_data:
				m_side_data.clear();
				mpi.bcast(m_side_data, head.value);
				break;
			case opt_code::r_side_data:
				r_side_data.clear();
				mpi.bcast(r_side_data, head.value);
				break;
			case opt_code::c_side_data:
				c_side_data.clear();
				mpi.bcast(c_side_data, head.value);
				break;
			}
		}

		template <typename V, typename T> static void
		setup(T &t, byte_array &bs, std::true_type) {
			t.setup(bs.read<V>());
			bs.reset();
		}
		template <typename, typename T> static void
		setup(T &, byte_array &, std::false_type) {}

		template <typename M> void *do_map(void *) {
			typedef map_func_type<M> map_func;
			typedef typename map_func::map_t map_t;
			typedef typename map_func::arg_t arg_t;
			typedef typename map_func::key_t key_t;
			typedef typename map_func::val_t val_t;
			typedef pair<key_t, val_t> pair_t;
			typedef collection<arg_t> arg_cc_t;
			typedef collection<pair_t> pair_cc_t;

			map_t mapper;
			setup<typename map_func::setup_t>(mapper, m_side_data, has_setup<map_t>());

			using std::hash;
			hash<key_t> hfunc;
			size_t size = mpi.size();
			pair_cc_t mid_cc_part, *pair_cc = new pair_cc_t[size];

			size_t count = mapped_data.read<size_t>();
			while ( count-- > 0 ) {
				arg_t part = mapped_data.read<arg_t>();
				mapper.map(part, mid_cc_part);
				for (pair_t &pair : mid_cc_part) {
					size_t target = (hfunc(pair.first) % size);
					pair_cc[target].push_back(std::move(pair));
				}
				mid_cc_part.clear();
			}
			mapped_data.reset();
			return pair_cc;
		}

		template <typename R> void *do_reduce(void *pair_cc_p) {
			typedef reduce_func_type<R> reduce_func;
			typedef typename reduce_func::reduce_t reduce_t;
			typedef typename reduce_func::key_t key_t;
			typedef typename reduce_func::val_t val_t;
			typedef typename reduce_func::ret_t ret_t;
			typedef pair<key_t, val_t> pair_t;
			typedef collection<val_t> val_cc_t;
			typedef collection<ret_t> ret_cc_t;
			typedef collection<pair_t> pair_cc_t;

			reduce_t reducer;
			setup<typename reduce_func::setup_t>(reducer, r_side_data, has_setup<reduce_t>());

			size_t size = mpi.size();
			pair_cc_t *pair_cc = (pair_cc_t *)pair_cc_p;

			byte_array send_data[size], recv_data[size];
			for (size_t k = 0; k < size; ++k) {
				send_data[k].write(pair_cc[k]);
				pair_cc[k].clear();
			}

			mpi.alltoall(send_data, recv_data);

			delete [] pair_cc;

			std::unordered_map<key_t, val_cc_t> middle_map;
			for (size_t k = 0; k < size; ++k) {
				byte_array &x = recv_data[k];
				pair_cc_t pairs = x.read<pair_cc_t>();
				for (pair_t &p : pairs) {
					middle_map[p.first].push_back(std::move(p.second));
				}
			}

			ret_cc_t ret_cc;
			for (auto &part : middle_map) {
				ret_cc.push_back(reducer.reduce(part.first, part.second));
			}

			byte_array *result = new byte_array();
			result->write(ret_cc);

			return result;
		}

		template <typename C> void *do_combine(void *pair_cc_p) {
			typedef combine_func_type<C> combine_func;
			typedef typename combine_func::combine_t combine_t;
			typedef typename combine_func::key_t key_t;
			typedef typename combine_func::val_t val_t;
			typedef typename combine_func::pair_t pair_t;
			typedef collection<val_t> val_cc_t;
			typedef collection<pair_t> pair_cc_t;

			combine_t combiner;
			setup<typename combine_func::setup_t>(combiner, c_side_data, has_setup<combine_t>());

			size_t size = mpi.size();
			pair_cc_t *pair_cc = (pair_cc_t *)pair_cc_p;

			for (size_t k = 0; k < size; k++) {
				std::unordered_map<key_t, val_cc_t> result_map;
				for (pair_t &pair : pair_cc[k]) {
					result_map[pair.first].push_back(std::move(pair.second));
				}
				pair_cc_t result;
				for (auto &pair : result_map) {
					result.push_back(combiner.combine(pair.first, pair.second));
				}
				pair_cc[k].swap(result);
			}
			return pair_cc;
		}
	};

	namespace work_flow_api {

		template <typename ... Ts> static void initialize() {
			MPI_Init(nullptr, nullptr);

			work_flow *wf = new work_flow();
			work_flow::instance() = wf;
			wf->hlist.push_back(nullptr);
			wf->register_type((Ts *)nullptr...);

			if ( wf->mpi.is_m() ) {
				atexit(work_flow::when_exit);
				at_quick_exit(work_flow::when_exit);
				return;
			}
			while ( true ) {
				wf->listen();
			}
		}

		template <typename T> static void scatter_map_data(const collection<T> &arg_cc) {
			work_flow::instance()->scatter(arg_cc);
		}

		template <typename T> static void set_map_side_data(const T &data) {
			work_flow *wf = work_flow::instance();
			wf->set_side_data(data, wf->m_side_data, opt_code::m_side_data);
		}

		template <typename T> static void set_reduce_side_data(const T &data) {
			work_flow *wf = work_flow::instance();
			wf->set_side_data(data, wf->r_side_data, opt_code::r_side_data);
		}

		template <typename T> static void set_combine_side_data(const T &data) {
			work_flow *wf = work_flow::instance();
			wf->set_side_data(data, wf->c_side_data, opt_code::c_side_data);
		}

		template <typename M, typename R = M, typename C = R>
		static typename job<M, R, C>::ret_cc_t run_without_scatter() {
			return work_flow::instance()->do_run<M, R, C>();
		}

		template <typename M, typename R = M, typename C = R>
		static typename job<M, R, C>::ret_cc_t run_job(const typename job<M, R, C>::arg_cc_t &arg_cc) {
			scatter_map_data(arg_cc);
			return run_without_scatter<M, R, C>();
		}
	}
}

#endif // _ARES_WORKFLOW_HPP_
