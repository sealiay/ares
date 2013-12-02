
#ifndef _ARES_CMD_HPP_
#define _ARES_CMD_HPP_

namespace ares_impl {

	enum class opt_code {
		map_data,
		m_side_data,
		r_side_data,
		c_side_data,
		start,
		exit
	};

	struct command {
		opt_code code;
		size_t value;
	};

}

#endif // _ARES_CMD_HPP_
