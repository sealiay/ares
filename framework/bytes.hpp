
#ifndef _ARES_BYTES_HPP_
#define _ARES_BYTES_HPP_

#include "types.hpp"

#include <string>

namespace ares_impl {

	typedef uint8_t byte;
	class byte_array;

	enum class serialize_type {
		trivial, serializable, unknow
	};

	template <typename T> using serialize_type_of =
			std::integral_constant<
			serialize_type,
			std::is_trivial<T>::value ?
			serialize_type::trivial :
			std::is_constructible<T, byte_array &>::value &&
			has_write_to<T>::value &&
			std::is_move_constructible<T>::value &&
			std::is_move_assignable<T>::value ?
			serialize_type::serializable :
			serialize_type::unknow>;

	template <typename, serialize_type> struct do_serialize;
	template <typename T> using serialize =
			do_serialize<T, serialize_type_of<T>::value>;

	class byte_array {
	private:
		std::vector<byte> _bytes;
		size_t _offset = 0;

	public:
		byte_array() = default;
		byte_array(byte_array &&) = default;

		const byte *read(size_t size) {
			byte *p = _bytes.data() + _offset;
			_offset += size;
			return p;
		}

		template <typename T> T read() {
			return serialize<T>::read(*this);
		}

		void write(const byte *bs, size_t size) {
			_bytes.insert(_bytes.end(), bs, bs + size);
		}

		template <typename T> void write(const T &v) {
			serialize<T>::write(v, *this);
		}

		const byte *data() const { return _bytes.data(); }

		size_t size() const { return _bytes.size(); }

		byte *reserve(size_t inc) {
			size_t s = _bytes.size();
			_bytes.reserve(s + inc);
			return _bytes.data() + s;
		}

		void reset() { _offset = 0; }

		void clear() {
			_bytes.clear();
			reset();
		}

	private:
		byte_array(const byte_array &) = delete;
		byte_array &operator=(const byte_array &) = delete;
	};

	template <typename T>
	struct do_serialize<T, serialize_type::trivial> {
		static T read(byte_array &bs) {
			const byte *p = bs.read(sizeof(T));
			return std::move(*reinterpret_cast<const T *>(p));
		}

		static void write(const T &v, byte_array &bs) {
			const byte *p = reinterpret_cast<const byte *>(&v);
			bs.write(p, sizeof(T));
		}
	};

	template <typename T>
	struct do_serialize<T, serialize_type::serializable> {
		static T read(byte_array &bs) {
			return T(bs);
		}

		static void write(const T &v, byte_array &bs) {
			v.write_to(bs);
		}
	};

	template <typename T>
	struct do_serialize<T, serialize_type::unknow> {
		static_assert(std::is_constructible<T, byte_array &>::value,
				"should be constructible from ares::byte_array &");
		static_assert(has_write_to<T>::value,
				"should have 'void write_to(ares::byte_array &)'");
		static_assert(std::is_move_constructible<T>::value,
				"type must be move constructible");
		static_assert(std::is_move_assignable<T>::value,
				"type must be move assignable");

		static T read(byte_array &bs);
		static void write(const T &v, byte_array &bs);
	};

	template <typename T>
	struct do_serialize<collection<T>, serialize_type::unknow> {
		static collection<T> read(byte_array &bs) {
			size_t s = bs.read<size_t>();
			collection<T> cc;
			cc.reserve(s);
			for (size_t i = 0; i < s; ++i) {
				cc.push_back(bs.read<T>());
			}
			return std::move(cc);
		}

		static void write(const collection<T> &cc, byte_array &bs) {
			bs.write(cc.size());
			for (const T &t : cc) {
				bs.write(t);
			}
		}
	};

	template <typename K, typename V>
	struct do_serialize<pair<K, V>, serialize_type::unknow> {
		static pair<K, V> read(byte_array &bs) {
			K k = bs.read<K>();
			V v = bs.read<V>();
			return make_pair(std::move(k), std::move(v));
		}

		static void write(const pair<K, V> &p, byte_array &bs) {
			bs.write(p.first);
			bs.write(p.second);
		}
	};

	template <typename C>
	struct do_serialize<std::basic_string<C>, serialize_type::unknow> {
		static std::basic_string<C> read(byte_array &bs) {
			size_t l = bs.read<size_t>();
			const byte *p = bs.read(l * sizeof(C));
			const C *s = reinterpret_cast<const C *>(p);
			return std::basic_string<C>(s, l);
		}

		static void write(const std::basic_string<C> &s, byte_array &bs) {
			bs.write(s.size());
			const byte *p = reinterpret_cast<const byte *>(s.data());
			bs.write(p, s.size() * sizeof(C));
		}
	};

}

#endif // _ARES_BYTES_HPP_
